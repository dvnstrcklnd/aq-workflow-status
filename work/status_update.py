import re
import json

import pandas as pd

from pydent import AqSession, __version__


def get_session(instance):
    with open('secrets.json') as f:
        secrets = json.load(f)

    credentials = secrets[instance]
    session = AqSession(
        credentials["login"],
        credentials["password"],
        credentials["aquarium_url"]
    )

    msg = "Connected to Aquarium at {} using pydent version {}"
    print(msg.format(session.url, str(__version__)))

    me = session.User.where({'login': credentials['login']})[0]
    print('Logged in as {}\n'.format(me.name))
    
    return session