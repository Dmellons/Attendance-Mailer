# SLUSD Attendance Mailer

## Setup

Create `.env` file in root directory with the following values or by copying `.env.example`

```.env
SERVER= # SIS Server Address
DATABASE= # SIS Database Name
DB_USERNAME= # SIS Database Username
DB_PASSWORD= # SIS Database Password
LOCAL_SMTP_SERVER= # Local SMTP Server Address (for sending emails)
```

## To Run

run `python ./main.py` from the root directory to generate data
All HTML templates are in the `html` directory
~~run `python mailer.py` from the root directory to send emails~~ <-- Mailer not yet implemented

## Notes

All queries are in the `sql_queries.py` file
All email templates are in the `html` directory
