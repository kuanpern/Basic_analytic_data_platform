import subprocess
import tempfile

# send email function
def utils_sendMail(emailUser, recipient, subject, text, attachFile = None):
    fp = tempfile.NamedTemporaryFile()
    fp.write(text.encode())
    fp.seek(0)

    cmd_template = '''mail -aFrom:{emailUser} -s "{subject}" --attach "{attachFile}" {recipient} < {textFile}'''
    cmd = cmd_template.format(
        emailUser = emailUser,
        recipient = recipient,
        subject   = subject,
        textFile  = fp.name,
        attachFile = attachFile
    )

    if attachFile == None:
        cmd = cmd.replace('--attach "None"', '')
    # end if

    process = subprocess.Popen(cmd, shell = True)
    process.wait()
# end def

sendmail = utils_sendMail
