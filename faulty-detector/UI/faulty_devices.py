import json
import matplotlib
import requests
import logging
import os
import boto3
import cognitojwt
import os
import pandas as pd
from flask import Flask, render_template, request, redirect, make_response, jsonify, url_for, session, send_file
from datetime import date
from flask_awscognito import AWSCognitoAuthentication
from flask_jwt_extended import set_access_cookies, get_jwt_identity, verify_jwt_in_request, JWTManager
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
from apscheduler.schedulers.background import BackgroundScheduler

from utils.s3_management import should_fetch_data, get_device_measures_with_success_by_date, \
    upload_df_to_s3, get_model_classifications_with_success_by_date, \
    get_external_classifications_with_success_by_date, get_latest_external_classifications_with_success, \
    get_latest_device_measures_with_success, get_latest_model_classifications_with_success
from utils.device_plot import build_image

matplotlib.pyplot.switch_backend('Agg')
app = Flask(__name__)
device_id_in_analysis = ''
host_in_use = "0.0.0.0"

app.config["AWS_DEFAULT_REGION"]='us-east-1'
app.config["AWS_COGNITO_DOMAIN"]='https://gps-faulty-detector.auth.us-east-1.amazoncognito.com'
app.config["AWS_COGNITO_USER_POOL_ID"]='us-east-1_IKKr42AM1'
app.config["AWS_COGNITO_USER_POOL_CLIENT_ID"]='62vohmr7842ej35r7jdp1cobi1'
app.config["AWS_COGNITO_USER_POOL_CLIENT_SECRET"]=''
app.config["AWS_COGNITO_REDIRECT_URL"]='https://34.199.169.162:5000/loggedin'
app.config["JWT_ACCESS_COOKIE_NAME"]='access_token_cookie'
app.config["JWT_COOKIE_SECURE"]=True
app.config["JWT_COOKIE_DOMAIN"]=None
app.config["JWT_ACCESS_COOKIE_PATH"]='/'
app.config["JWT_COOKIE_SAMESITE"]=None
app.config["JWT_TOKEN_LOCATION"]=["cookies"]
app.config["JWT_COOKIE_CSRF_PROTECT"]=True
app.config["JWT_CSRF_IN_COOKIES"]=True
app.config["JWT_ACCESS_CSRF_COOKIE_NAME"]='csrf_access_token'
app.config["JWT_ACCESS_CSRF_COOKIE_PATH"]='/'
app.config["SECRET_KEY"]=os.urandom(16)

REGION = 'us-east-1'
USER_POOL_ID = 'us-east-1_IKKr42AM1'
APP_CLIENT_ID='62vohmr7842ej35r7jdp1cobi1'
SECRET_KEY = os.urandom(24)
app.secret_key= SECRET_KEY
aws_auth = AWSCognitoAuthentication(app)
jwt = JWTManager(app)
COGNITO_CLIENT_ID='62vohmr7842ej35r7jdp1cobi1'
global LAST_UNIT_ID_CONSULTED


@app.route("/", methods=['GET'])
def sign_in():
    """
    :return: returns the rendered html template for the sign in
    """
    
    return redirect(aws_auth.get_sign_in_url())
   
def auth_required(func):
    def wrapper(*args, **kwargs):
        try: 
            access_token = request.cookies.get('access_token')
            if not validate_cognito_access_token(access_token):
                
                return redirect(aws_auth.get_sign_in_url())

        except Exception as e:
            
            return redirect(aws_auth.get_sign_in_url())

        return func(*args, **kwargs)

    return wrapper
       
 
def validate_cognito_access_token(access_token):

    verified_claims: dict = cognitojwt.decode(
        access_token,
        REGION,
        USER_POOL_ID,
        app_client_id=APP_CLIENT_ID,
        testmode= True
    )
    
    return verified_claims


@app.route("/loggedin", methods=["GET"])
def logged_in(): 
    access_token = aws_auth.get_access_token(request.args)
    resp = make_response(redirect(url_for("home")))
    resp.set_cookie('access_token', access_token)

    return resp
 
    
@app.route("/home", methods=['GET'])
def home():
    """
    :return: returns the rendered html template for the homepage.
    """
    
    try:
        access_token = request.cookies.get('access_token')
        if validate_cognito_access_token(access_token):
        
            return render_template("index.html",
                           scatter_visibility='none',
                           invalid_id_error_visibility='none',
                           not_existing_device_error_visibility='none',
                           unit_server_times_visibility='none')
    
        else:
            return redirect(aws_auth.get_sign_in_url())
    except Exception as e:
        print(e)
        return redirect(aws_auth.get_sign_in_url())


 
def redirect_to_login():

    return redirect('https://gps-faulty-detector.auth.us-east-1.amazoncognito.com/login?client_id=4efgimo7313eri6irnv32s3a54&response_type=code&scope=aws.cognito.signin.user.admin+email+openid+phone&redirect_uri=https://34.199.169.162:5000/home')


@app.route('/my_function', methods=['POST'])
def my_function():
    try:
        selected_date = request.form['fecha']
        return see_device_id_history_and_calssification(str(selected_date))
        
        if not is_date_valid(selected_date):
            return 'Not valid'
        
        return selected_date, type(selected_date)

    except Exception as e:
        return e

def is_date_valid(date):

    return True

       
       
@app.route('/download', methods=['POST'])
def download_file():
    global LAST_UNIT_ID_CONSULTED 
    date = request.form['date']
    logging.info(f"Downloading file for {LAST_UNIT_ID_CONSULTED} from date {date}") 
    try:
        return send_file(f"{LAST_UNIT_ID_CONSULTED}_{date}.csv", as_attachment=True)
    except Exception as e:
        pass


@app.route('/download_external', methods=['POST'])
def download_external_classifications_file():
     
    today = date.today()
    today_str = today.strftime("%Y-%m-%d") 
     
    try:
        return send_file(f"External_classifications_{today_str}.csv", as_attachment=True)
    except Exception as e:
        pass

@app.route('/device-id-history', methods=['POST'])
def see_device_id_history_and_classification(parameter_date=None):
    """
    Renders the html template showing the scatter with the history of a device by device id,
    its classification by all three models (Statistical high, Statistical low,
    Anomaly) and overall classification by faulty detector model. It also includes
    a form to classify the device and add a comment to provide further insight
    for metamodel improvements.
    :return: template showing device history, classification and form.
    """
    global device_id_in_analysis
    global measures_df
    global models_classifications
    global measures_df_dict
    global models_classifications_dict
    global last_file_name
 
    logging.info(f"Device id history")
    selected_date = None
    today = date.today()
    today_str = today.strftime("%Y-%m-%d") 

    try: 
        selected_date = request.form['date']
        selected_date_str = selected_date
        logging.info(selected_date) 
        if selected_date == "":
            selected_date = today
            selected_date_str = today_str       
    
    except Exception as e:  
        selected_date = today
        selected_date_str = today_str    
    
    logging.info(f"Date: {selected_date_str}")
    
    try:
        device_id = int(request.form.get('deviceId'))
        device_id_in_analysis = device_id
        logging.info(f'Getting device id from form')

    except ValueError:
     
        return render_template("index.html",
                               scatter_visibility='none',
                               invalid_id_error_visibility='inline',
                               not_existing_device_error_visibility='none',
                               unit_server_times_visibility='none')
    except Exception as e:
          
        device_id_in_analysis = device_id_in_analysis
        logging.info(f'Getting device id from last device id consulted')
        logging.info(e)
    
    logging.info(f"Device id: {device_id_in_analysis}")
    
    try: 
        unit_id = request.form.get('unitId')
        logging.info(f'Unit id from form') 
    except Exception as e:
        unit_id = None
        logging.info(f'Getting this error when trying to get unit id {e}')

    logging.info(f"Unit id: {unit_id}") 
     
    if measures_df_dict.get(today_str) is None: 
        get_data()
        logging.info(f"There is no data retrieved for: {today_str}") 

    if selected_date_str is not None:   
        if measures_df_dict.get(selected_date_str) is not None:   
            measures_df = measures_df_dict.get(selected_date_str)
       
        else:     
            logging.info(f"There is no data retrieved for: {selected_date_str}")  
            measures_df = get_device_measures_with_success_by_date(selected_date)
           
    if not device_exists(measures_df): 
        logging.info(f"Data for {selected_date_str} is empty")
        if unit_id is None:
            unit_id = ''

        return render_template("index.html",
                               scatter_visibility='none',
                               invalid_id_error_visibility='none',
                               not_existing_device_error_visibility='none',
                               unit_server_times_visibility='none',
                               device_id=device_id_in_analysis,
                               unit_id=unit_id,
                               selected_date=selected_date,
                               download_visibility='none')
    
    logging.info(f"Start model classification")

    overall_faulty, stat_low_faulty, stat_high_faulty, anomaly_faulty, late_server_time = models_classification(
        device_id_in_analysis, selected_date_str)
    
    device_filter = measures_df["deviceId"] == int(device_id_in_analysis)
    device_df = measures_df[device_filter].drop_duplicates(ignore_index=True)
    device_df['unitTime'] = pd.to_datetime(device_df['unitTime'], utc=False)

    scatter_path = build_image(device_id_in_analysis, device_df)
    
    unit_id = device_df['unitId'].iloc[0]
    if unit_id == None:
        unit_id = ''
    
    global LAST_UNIT_ID_CONSULTED
    LAST_UNIT_ID_CONSULTED = unit_id

    try: 
        os.remove(last_file_name)

    except Exception as e:
        pass
     
    file_name = f"{LAST_UNIT_ID_CONSULTED}_{selected_date}.csv"
    last_file_name = file_name
    
    try: 
        device_df.to_csv(file_name, index=False)

    except NameError as e:
        pass

    return render_template('index.html',
                           scatter_visibility='inline',
                           invalid_id_error_visibility='none',
                           not_existing_device_error_visibility='none',
                           graph_name=scatter_path,
                           stat_high_classification=status(stat_high_faulty),
                           stat_low_classification=status(stat_low_faulty),
                           anomaly_classification=status(anomaly_faulty),
                           late_server_time=late_server_time,
                           overall_classification=status(overall_faulty),
                           unit_server_times_visibility='none',
                           device_id=device_id_in_analysis,
                           unit_id=unit_id,
                           selected_date=selected_date,
                           download_visibility='inline')


@app.route('/unit-id-history', methods=['POST'])
def see_unit_id_history_and_classification():
    """
    Renders the html template showing the scatter with the history of a device by unit id,
    its classification by all three models (Statistical high, Statistical low,
    Anomaly) and overall classification by faulty detector model. It also includes
    a form to classify the device and add a comment to provide further insight
    for metamodel improvements.
    :return: template showing device history, classification and form.
    """
    global device_id_in_analysis
    global measures_df
    global measures_df_dict

    logging.info(f'Unit id history')

    unit_id = request.form['unitId']
    logging.info(f'Unit id: {unit_id}')
    
    if unit_id == '':
        logging.info(f'Empty unit id')
        return render_template("index.html",
                               scatter_visibility='none',
                               invalid_id_error_visibility='inline',
                               not_existing_device_error_visibility='none',
                               unit_server_times_visibility='none') 

    df_filtered_unit_id = None

    for key in measures_df_dict.keys():
        
        measures = measures_df_dict.get(key) 
        df_filtered_unit_id = measures[measures['unitId'] == unit_id]
        if len(df_filtered_unit_id) != 0:
            logging.info(f'For date {key} the unit id {unit_id} is not empty')
            break

    if len(df_filtered_unit_id) == 0 or df_filtered_unit_id is None:
        logging.info(f'Unit id: {unit_id} has no measures on the last 5 days')

        return render_template("index.html",
                               scatter_visibility='none',
                               invalid_id_error_visibility='none',
                               not_existing_device_error_visibility='inline',
                               unit_server_times_visibility='none')

    device_id = df_filtered_unit_id['deviceId'].iloc[0]
    request_params = {'deviceId': device_id, 'unitId': unit_id}

    logging.info(f'Calling device id history with params: {request_params}')
    disable_warnings(InsecureRequestWarning)
    return requests.post("https://0.0.0.0:5000/device-id-history", data=request_params, verify=False).content
    


@app.route('/server-unit-times', methods=['POST'])
def see_server_unit_faulty_times():
    """
    Renders the html template showing both the serverTime and unitTime for the device, in which the server time
    was 3 days past from the unitTime.
    """
    global device_id_in_analysis
    global measures_df
    global models_classifications
    device_id = int(request.form['deviceId'])
    device_id_in_analysis = device_id

    device_classification = models_classifications[models_classifications['deviceId'] == int(device_id_in_analysis)]
    server_time_failures = device_classification['serverTimeFailures'].iloc[0]
    server_time_failures = eval(server_time_failures)

    return render_template('index.html',
                           scatter_visibility='none',
                           invalid_id_error_visibility='none',
                           not_existing_device_error_visibility='none',
                           unit_server_times_visibility='inline',
                           list_unit_server_times=server_time_failures,
                           device_id=device_id_in_analysis)


@app.route('/list-faulty', methods=['GET'])
def list_of_faulty():
    """
    :return: returns the rendered html template showing the list of all faulty devices classified by the model
    in latest successful run of etl.
    """
    try:
        access_token = request.cookies.get('access_token')
        
        if not validate_cognito_access_token(access_token): 

            return redirect(aws_auth.get_sign_in_url())
    except Exception as e:
        print(e)
        return redirect(aws_auth.get_sign_in_url())

    global measures_df
    global models_classifications
    global measures_df_dict
    global models_classifications_dict
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
      
    if measures_df_dict.get(today_str) is None:
        get_data() 

    faulty_devices = models_classifications[models_classifications['deviceStatus']].drop_duplicates()
    late_server_time_devices = models_classifications[models_classifications['lateServerTime']].drop_duplicates()
    classified_devices = get_external_classifications_with_success_by_date(today_str).drop_duplicates()

    file_name = f"External_classifications_{today_str}.csv"
    last_file_name = file_name 

    try: 
        os.remove(last_file_name)

    except Exception as e:
        pass

    try: 
        classified_devices.to_csv(file_name, index=False)

    except NameError as e:
        pass

    list_faulty_json = json.dumps(faulty_devices['deviceId'].tolist())
    list_server_time_json = json.dumps(late_server_time_devices['deviceId'].tolist())
    list_classified_json = json.dumps(list(set(classified_devices['deviceId'])))

    return render_template('faulty_devices.html', list_of_faulty=list_faulty_json,
                           list_server_time=list_server_time_json, list_classified=list_classified_json)


@app.route('/classifications', methods=['POST'])
def classify_device():
    """
    Adds new classification to the external_classifications file. A single device can have multiple classifications
    in external_classification file.
    :return: returns the rendered html template for the homepage.
    """
    global external_classifications_df_dict 
    global external_classifications_df 

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    classification_comment = request.form.get("comment-faulty")
    is_faulty = request.form.get('faulty') is not None

    external_classifications_df = get_external_classifications_with_success_by_date(today) 
    new_classification = [device_id_in_analysis, today, is_faulty, classification_comment]
    external_classifications_df.loc[len(external_classifications_df)] = new_classification

    upload_df_to_s3(external_classifications_df, 'external_classifications')

    return home()


def get_data():
     
    global measures_df
    global models_classifications
    global measures_df_dict
    global models_classifications_dict
    global external_classifications_df_dict 
    global external_classifications_df 
   
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")  

    measures_df_dict = get_latest_device_measures_with_success(date.today(), 5)
    measures_df = measures_df_dict.get(today_str)
    models_classifications_dict = get_latest_model_classifications_with_success(date.today(), 5)
    models_classifications = models_classifications_dict.get(date.today())
    external_classifications_df_dict = get_latest_external_classifications_with_success(date.today(), 5)
    external_classifications_df = external_classifications_df_dict.get(date.today())


def device_exists(measures_df):
    """
    :param measures_df: dataframe with devices measures for the previous 7 days.
    :return: True if device exists in readings, False otherwise.
    """
    if len(measures_df[measures_df['deviceId'] == int(device_id_in_analysis)]) == 0:
        return False
    return True


def status(model_status):
    """
    :param model_status: boolean status of the model.
    :return: maps boolean status to faulty or not faulty.
    """
    if model_status:
        return 'Faulty'
    else:
        return 'not Faulty'


def models_classification(device_id, selected_date_str):
    """
    Returns the classification of the device by all three models (Statistical high, Statistical low,
    Anomaly) and overall classification by faulty detector model.
    :param device_id: the id of the device to be classified.
    :return: Returns a bool (classification) for each of the individual models and the metamodel.
    """
    global models_classifications
    global measures_df
    global measures_df_dict
    global models_classifications_dict
    
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    if measures_df_dict.get(today_str) is None:
        get_data() 

    try:
        logging.info(f'date: {selected_date_str}')
        keys = models_classifications_dict.keys()
        logging.info(f'keys: {keys}')
        

    except Exception as e:
        pass 

    selected_date_classifications = models_classifications_dict.get(selected_date_str)
    logging.info(selected_date_classifications)
    device_classification = selected_date_classifications[selected_date_classifications['deviceId'] == device_id] 
    logging.info(device_classification)
    stat_low_status = device_classification['statLowStatus'].iloc[0]
    stat_high_status = device_classification['statHighStatus'].iloc[0]
    anomaly_status = device_classification['anomalyStatus'].iloc[0]
    late_server_time = device_classification['lateServerTime'].iloc[0]
    device_status = device_classification['deviceStatus'].iloc[0]
    return bool(device_status), bool(stat_low_status), bool(stat_high_status), bool(anomaly_status), \
           bool(late_server_time)


def hourly_data_retrieval():
    global measures_df_dict
    global measures_df
    global models_classifications_dict
    global models_classifications
    global external_classifications_df_dict
    global external_classifications_df

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    
    measures_df_dict[today_str] = get_device_measures_with_success_by_date(today)
    measures_df = measures_df_dict.get(today_str)

    models_classifications_dict[today_str] = get_model_classifications_with_success_by_date(today)
    models_classifications = models_classifications_dict.get(today_str)

    external_classifications_df_dict[today_str] = get_external_classifications_with_success_by_date(today)
    external_classifications_df = external_classifications_df_dict.get(today_str)

    return
    

def daily_data_retrieval():

    global measures_df_dict
    global measures_df
    global models_classifications_dict
    global models_classifications
    global external_classifications_df_dict
    global external_classifications_df

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    dates_str = list(measures_of_dict.keys())
    dates_datetime = [datetime.strptime(date, '%Y-%m-%d') for date in date_str]
    oldest_date = min(dates_datetime)
    oldest_date_str = oldest_date.strftime("%Y-%m-%d")

    measures_df_dict[oldest_date_str] = get_device_measures_with_success_by_date(today)
    measures_df = measures_df_dict.get(today_str)

    models_classifications_dict[oldest_date_str] = get_model_classifications_with_success_by_date(today)
    models_classifications = models_classifications_dict.get(today_str)

    external_classifications_df_dict[oldest_date_str] = get_external_classifications_with_success_by_date(today)
    external_classifications_df = external_classifications_df_dict.get(today_str)
    
      
    return


scheduler = BackgroundScheduler()
scheduler.add_job(hourly_data_retrieval, 'cron', minute=30)
scheduler.add_job(daily_data_retrieval, 'cron', hour=5)
scheduler.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.info("Starting")
 
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    
    measures_df_dict = get_latest_device_measures_with_success(date.today(), 5)
    logging.info(measures_df_dict.keys())
    measures_df = measures_df_dict.get(today_str) 
    models_classifications_dict = get_latest_model_classifications_with_success(date.today(), 5)
    logging.info(models_classifications_dict.keys())
    models_classifications = models_classifications_dict.get(today_str) 
    external_classifications_df_dict = get_latest_external_classifications_with_success(date.today(), 5)
    logging.info(external_classifications_df_dict.keys())
    external_classifications_df = external_classifications_df_dict.get(today_str) 
    app.run(host=host_in_use, ssl_context=('cert.pem','key.pem'), debug=True)
    
    
