import pytz

ENV_TZ = {
    'icds': pytz.timezone('Asia/Kolkata'),
    'icds-new': pytz.timezone('Asia/Kolkata'),
    'prod': pytz.utc,
    'production': pytz.utc,
    'enikshay': pytz.timezone('Asia/Kolkata'),
    'softlayer': pytz.timezone('Asia/Kolkata'),
    'india': pytz.timezone('Asia/Kolkata'),
    'swiss': pytz.timezone('Europe/Zurich'),
}


DATADOG_ENVS = list(ENV_TZ.keys())
