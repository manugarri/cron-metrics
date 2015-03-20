import sys
import os
from datetime import datetime

#parent imports
env_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, os.pardir))
sys.path.append(env_path)
from utils.send_email import send_email


def test_main():
    '''Main process for the Lead finder app'''
    print('starting test task')
    start = datetime.now()
    send_email(to = ['test@test.com'], subject = 'TEST CRON', text =  '{}'.format(start))

if __name__ == '__main__':
    if os.environ.get('CRAB') == 'true':
        from utils.utils import crab_task
        with crab_task(__file__):
            test_main()
    else:
        test_main()
