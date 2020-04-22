import sys
import imaplib
import getpass
import email
import email.header
import email.message
import datetime
import pandas as pd
import logging


def gmail_to_csv(username,password,imap_server,inbox_label,csv_file_path):

    message_ids = []
    day_list = []
    date_list = []
    sender_list = []
    subject_list =[]
    body_list = []

    mail = imaplib.IMAP4_SSL(imap_server)

    # connecting to the imap server
    try:
        rv, data = mail.login(username, password)
    except imaplib.IMAP4.error:
        logging.info ("LOGIN FAILED!!! ")
        sys.exit(1)

    # selecting the inbox label to read
    rv, data = mail.select(inbox_label)
    if rv == 'OK':
        logging.info("Processing mailbox...\n")
    else:
        logging.info("ERROR: Unable to open mailbox ", rv)

    # searching for messages withing the inbox label
    rv, data = mail.search(None, "ALL")
    if rv != 'OK':
        logging.info("No messages found!")
        return

    # reading each of the emails
    for num in data[0].split():
        rv, data = mail.fetch(num, '(RFC822)')
        message_id = int(num.decode('utf-8'))
        if rv != 'OK':
            logging.info("ERROR getting message", num)
            return

        msg = email.message_from_bytes(data[0][1])
        hdr = email.header.make_header(email.header.decode_header(msg['Subject']))
        sender = msg['From']

        message_ids.append(message_id)
        sender_list.append(sender)
        subject_list.append(hdr)

        # Now convert to local date-time
        date_tuple = email.utils.parsedate_tz(msg['Date'])
        if date_tuple:
            local_date = datetime.datetime.fromtimestamp(
                email.utils.mktime_tz(date_tuple))
            date_list.append(local_date.strftime("%m/%d/%Y"))
            day_list.append(local_date.strftime("%A"))

        # reading the body of the emails
        for  part in msg.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True)
                message_body = body.decode('utf-8')
                if 'Coding' in msg['From']:
                    body_list.append('*Coding Challenge* \n\n' + message_body.partition('\n---')[0])                    
                elif 'Product' in msg['From'] :
                    body_list.append('*Product Management Prep* \n\n' + message_body.partition('\n---')[0])
                else:
                    body_list.append('*Data Science Prep* \n\n' + message_body.partition('\n---')[0])
        
    # adding columns to dataframe
    df = pd.DataFrame(data = {'Message_id': message_ids, 'Day_of_week':day_list, 'Date':date_list, 'Sender':sender_list, 'Subject':subject_list,'Body':body_list})

    # writing the completed dataset to a csv file
    df.to_csv(csv_file_path,index=False)

    mail.close()
    mail.logout()

