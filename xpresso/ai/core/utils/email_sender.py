__all__ = ['XprEmailSender']
__author__ = 'Srijan Sharma'

from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import EmailException
import smtplib
from email.utils import formatdate


class XprEmailSender():

    EMAIL_SECTION = "email_notification"
    PROJECT_NAME = "Project Name"
    SMTP_HOST = "smtphost"
    SMTP_PORT = "smtpport"
    SENDER_MAIL = "sender_mail"
    SENDER_PASSWD = "sender_passwd"

    def __init__(self):
        self.xpr_config = XprConfigParser(config_file_path=XprConfigParser.DEFAULT_CONFIG_PATH)
        self.logger = XprLogger()

    def send_single_mail(self, receiver, message, subject):
        smtp_session = None
        try:
            smtp_session = smtplib.SMTP(self.xpr_config[self.EMAIL_SECTION][
                                 self.SMTP_HOST],self.xpr_config[
                self.EMAIL_SECTION][self.SMTP_PORT])
            smtp_session.starttls()

            smtp_session.login(self.xpr_config[self.EMAIL_SECTION][self.SENDER_MAIL],
                    self.xpr_config[self.EMAIL_SECTION][self.SENDER_PASSWD])

            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.xpr_config[self.EMAIL_SECTION][self.SENDER_MAIL],
                receiver, subject, formatdate(), message)

            smtp_session.sendmail(self.xpr_config[self.EMAIL_SECTION][self.SENDER_MAIL],
                       receiver, msg)
            self.logger.info("{}  Successfully sent to {}".format(msg,
                                                                  receiver))

        except smtplib.SMTPServerDisconnected as e:
            err = "server unexpectedly disconnects:{}".format(e)
            self.logger.error(err)
            raise EmailException(err)

        except smtplib.SMTPSenderRefused as e:
            err = "Sender address refused : {}".format(e)
            self.logger.error(err)
            raise EmailException(err)

        except smtplib.SMTPRecipientsRefused as e:
            err = "recipient {} addresses refused : {}".format(receiver,e)
            self.logger.error(err)
            raise EmailException(err)

        except smtplib.SMTPDataError as e:
            err = "The SMTP server refused to accept the message data. :{}".format(e)
            self.logger.error(err)
            raise EmailException()

        except smtplib.SMTPConnectError as e:
            err = "Error connecting to server : {}".format(e)
            self.logger.error(err)
            raise EmailException(err)

        except smtplib.SMTPAuthenticationError as e:
            err = "Unable to authenticate : {}".format(e)
            self.logger.error(err)
            raise  EmailException(err)

        except smtplib.SMTPException as e:
            err = "Error sending mail :{}".format(e)
            self.logger.error(err)
            raise EmailException(err)

        finally:
            if smtp_session is not None:
                smtp_session.quit()

    def send(self, receivers, message, subject):
        success = list()
        failure = list()
        for receiver in receivers:
            try:
                self.send_single_mail(receiver, message, subject)
                success.append(receiver)
            except EmailException as e:
                failure.append(receiver)
        self.logger.info('successfully send email to {} '.format(str(success)))
        self.logger.info('Unable to send email to {} '.format(str(failure)))
        return failure


if __name__ == "__main__":
    sender = XprEmailSender()
    sender.send(["##"], "Test Message", "Test subject")
