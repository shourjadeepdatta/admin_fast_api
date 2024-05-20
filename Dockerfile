FROM python:2.7.12
ENV TZ="Asia/Kolkata"
RUN date
FROM tiangolo/meinheld-gunicorn:python2.7
WORKDIR /usr/admin-portal-backend_v2
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5568

CMD ["gunicorn","--config","./gunicorn.conf.py", "main:app"]
#CMD gunicorn --bind 0.0.0.0:5568 --log-level debug main:app --access-logfile ipv_logs --error-logfile error_file --capture-output  --workers 2 --worker-class="egg:meinheld#gunicorn_worker"

