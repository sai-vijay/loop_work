To be installed in Machine: 
python, Django, gdown, pandas, git.

To Run Commands:
on the main repo run the following command  
$python manage.py runserver
To call the api open web browser and at address write  "http://127.0.0.1/trigger_report/"
it gives the string report id 
Now call the other api "http://127.0.0.1/get_report/{report_id}" replace the {report_id} with the string we get in previous api we get the display of output a report. 


