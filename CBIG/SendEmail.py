import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import DatabaseConfigFile as dbcfg
import logging
from datetime import date, tzinfo, timedelta

class ErrorLog:

    def SetLoggingInformation(self):
        logging.basicConfig(level=logging.INFO, filename=dbcfg.LogFile["LogFileDIR12"],
                            format='[%(asctime)s] p%(process)s %(lineno)d %(levelname)s - %(message)s',
                            datefmt='%m-%d %H:%M:%S',
                            filemode='a')
        logger = logging.getLogger()

        return logger

    def setup_logger(self, logger_name, log_file, level=logging.INFO):
        # logging.basicConfig(logger_name,format='[%(asctime)s] [%(filename)s:%(lineno)s - %(funcName)20s()] %(levelname)s - %(message)s',datefmt='%m-%d %H:%M:%S',filemode='a',level="INFO")
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(filename)s:%(lineno)s - %(funcName)20s()] %(levelname)s - %(message)s',
            datefmt='%m-%d %H:%M:%S')
        fileHandler = logging.FileHandler(log_file, mode='a')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        logger.setLevel(level)
        logger.addHandler(fileHandler)
        logger.addHandler(streamHandler)
        return logger

class SendEmail:

    def SendEmailToStakeHolders(self,Subject,FromMail,recipient,dfhtml,StatsType,LogPath):
        try:

            msg = MIMEMultipart()
            # msg = email.message.Message()
            # msg['Subject'] = 'AOC PDF Parser Status ' + str(date.today())
            msg['Subject'] = str(Subject)+ str(date.today())
            # msg['From'] = "DNBSystemMailDoNotReply@dnb.com"
            msg['From'] =FromMail
            recipients = recipient
            # recipients = 'shethd@dnb.com,BaneY@dnb.com'
            # recipients = 'GowdaS@DNB.com,shethd@dnb.com'
            msg['To'] = recipients
            msg.preamble = 'preamble'

            if(len(dfhtml) != 0):
                email_content = """
                <html>
                <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    
                   <title>Defaulter's List</title>
                   <style type="text/css">
                    a {color: #d80a3e;}
                  body, #header h1, #header h2, p {margin: 0; padding: 0;}
                  #main {border: 1px solid #cfcece;}
                  img {display: block;}
                  #top-message p, #bottom p {color: #3f4042; font-size: 12px; font-family: Arial, Helvetica, sans-serif; }
                  #header h1 {color: #ffffff !important; font-family: "Lucida Grande", sans-serif; font-size: 24px; margin-bottom: 0!important; padding-bottom: 0; }
                  #header p {color: #ffffff !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; font-size: 12px;  }
                  h5 {margin: 0 0 0.8em 0;}
                    h5 {font-size: 18px; color: #444444 !important; font-family: Arial, Helvetica, sans-serif; }
                  p {font-size: 12px; color: #444444 !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; line-height: 1.5;}
                   </style>
                </head>
    
                <body>
    
    
                <table width="100%" cellpadding="0" cellspacing="0" bgcolor="e4e4e4"><tr><td>
    
                <table id="main" width="600" align="center" cellpadding="0" cellspacing="15" bgcolor="ffffff">
                    <tr>
                      <td>
                        <table id="header" cellpadding="10" cellspacing="0" align="center" bgcolor="8fb3e9">
                          <tr>
                            <td width="570" align="center"  bgcolor="#3095b4"><h1>DUN AND BRADSTREET</h1></td>
                          </tr>
                          <tr>
                            <td width="570" bgcolor="#3095b4" align="center"><p>""" + str(date.today()) + """</p></td>
                          </tr>
                        </table>
                      </td>
                    </tr>
    
                    <tr>
                      <td>
                        <table id="content-3" cellpadding="0" cellspacing="0">
                          <tr>
                              <td width="250" valign="top" style="padding:5px;">
                              Hi Team, 
                              </td>   
                          </tr>
                          <tr>
                          <td style="padding:15px;">
                          This is to inform you that """+StatsType+""" Parser process has been completed on date """+str(date.today())+"""
                          </td>               
                          </tr>
                          <tr>
                          <td style="padding:15px;">
                          SharePoint link to access logs and Stats : <br />
                          <a href="""+LogPath+""">Logs</a>
                          </td> 
                          </tr>
                        </table>
                      </td>
                    </tr>
                    
                    <tr>
                    <td style="padding:15px;">
                          """+StatsType+""" Stats <br /><br />"""+str(dfhtml)+"""
                          </td>  
                    </tr>
                    <tr>
                      <td>
                         <b>Best regards,<br /></b>
                        <br />
                        <img src="""+dbcfg.ImagePath["ImagePath"]+"dnbSiganature.png"+"""/><br />
                      </td>
                    </tr>
                  </table>
                <!-- wrapper -->
                </body>
                </html>"""
            else:
                email_content = """
                                <html>
                                <head>
                                <meta http-equiv="Content-Type" content="text/html; charset=utf-8">

                                   <title>Defaulter's List</title>
                                   <style type="text/css">
                                    a {color: #d80a3e;}
                                  body, #header h1, #header h2, p {margin: 0; padding: 0;}
                                  #main {border: 1px solid #cfcece;}
                                  img {display: block;}
                                  #top-message p, #bottom p {color: #3f4042; font-size: 12px; font-family: Arial, Helvetica, sans-serif; }
                                  #header h1 {color: #ffffff !important; font-family: "Lucida Grande", sans-serif; font-size: 24px; margin-bottom: 0!important; padding-bottom: 0; }
                                  #header p {color: #ffffff !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; font-size: 12px;  }
                                  h5 {margin: 0 0 0.8em 0;}
                                    h5 {font-size: 18px; color: #444444 !important; font-family: Arial, Helvetica, sans-serif; }
                                  p {font-size: 12px; color: #444444 !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; line-height: 1.5;}
                                   </style>
                                </head>

                                <body>


                                <table width="100%" cellpadding="0" cellspacing="0" bgcolor="e4e4e4"><tr><td>

                                <table id="main" width="600" align="center" cellpadding="0" cellspacing="15" bgcolor="ffffff">
                                    <tr>
                                      <td>
                                        <table id="header" cellpadding="10" cellspacing="0" align="center" bgcolor="8fb3e9">
                                          <tr>
                                            <td width="570" align="center"  bgcolor="#3095b4"><h1>DUN AND BRADSTREET</h1></td>
                                          </tr>
                                          <tr>
                                            <td width="570" bgcolor="#3095b4" align="center"><p>""" + str(
                    date.today()) + """</p></td>
                                          </tr>
                                        </table>
                                      </td>
                                    </tr>

                                    <tr>
                                      <td>
                                        <table id="content-3" cellpadding="0" cellspacing="0">
                                          <tr>
                                              <td width="250" valign="top" style="padding:5px;">
                                              Hi Team, 
                                              </td>   
                                          </tr>
                                          <tr>
                                          <td style="padding:15px;">
                                          This is to inform you that """+StatsType+""" process has been completed on date """ + str(
                    date.today()) + """
                                          </td>               
                                          </tr>
                                          <tr>
                                          <td style="padding:15px;">
                                          SharePoint link to access logs : <br />
                                          <a href="""+LogPath+""">Logs</a>
                                          </td> 
                                          </tr>
                                        </table>
                                      </td>
                                    </tr>

                                    <tr>
                                    <td style="padding:15px;">
                                          """+StatsType+""" Stats :=> <br /><br />""" + "No files to process, please make sure folder and files exists on the sharepoint" + """
                                          </td>  
                                    </tr>
                                    <tr>
                                      <td>
                                         <b>Best regards,<br /></b>
                                        <br />
                                        <img src=""""" + dbcfg.ImagePath["ImagePath"] + "dnbSiganature.png" + """""/><br />
                                      </td>
                                    </tr>
                                  </table>
                                <!-- wrapper -->
                                </body>
                                </html>"""

            body = MIMEText(email_content, "html")
            msg.attach(body)

            server = smtplib.SMTP(dbcfg.SMTPServer["External_SMTP"], dbcfg.SMTPServer["Port"])
            server.ehlo()
            server.starttls()
            # server.login(msg['From'],'')
            server.sendmail(msg['From'], recipients.split(',') , msg.as_string())
            server.quit()

        except Exception as ex:
            logging.error("Email Error")
            logging.error(str(ex))
            quit()
