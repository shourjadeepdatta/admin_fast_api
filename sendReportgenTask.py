from celery import Celery
app_celery_new = Celery('celery',
                  broker='amqp://abhiace:abhi123$@ec2-13-233-10-152.ap-south-1.compute.amazonaws.com/kwikid_vkyc_rbl'
                  )
def celery_generate_report(client_name, start_time, end_time, kind, get_latest_status_for_userid_only):
    print("celery_generate_report  called")
    request_id = app_celery_new.send_task(name='generate_report.task',
                             args=[client_name, start_time, end_time, kind, get_latest_status_for_userid_only],
                             queue='celery_report_maker')
    print("celery_generate_report request_id = ", request_id)
    return request_id 

celery_generate_report('RBL', 1701078133, 1701164533, '', True)
