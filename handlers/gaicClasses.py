#
# # GAIC dglContactsClasses
#
#
# # class FirmEmails - list of emails identified as going to firms rather
# # than individuals
# # class loads list from file at __init__
#


class FirmEmails():
    def __init__(self):
        try:
            """ CHANGE GO GET DOMAINS LIST FROM BUCKET """
            f = open("/home/les/Downloads/dgl-FirmDomains.csv", "r")
            self.firm_emails = []
            self.firm_emails = f.read().split("@")  # Read list of domains
        except (FileNotFoundError, f.Error) as e:
            print(e)
            print(e.args)
            quit(False)

#
# # Return true if email_domain is in firm_emails
#

    def inFirmEmails(self, email_domain):
        if email_domain in self.firm_emails:
            return True
        else:
            return False
