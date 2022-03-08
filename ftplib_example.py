from ftplib import FTP
from datetime import datetime

start = datetime.now()
ftp = FTP('hostname')
ftp.login('user','pwd')
ftp.cwd('/path/to/files')

files = ftp.nlst()
for file in files:
    print("Downloading..." + file)
    ftp.retrbinary("RETR " + file ,open("../var/"+file, 'wb').write)

ftp.close()

end = datetime.now()
diff = end - start
print('All files downloaded for ' + str(diff.seconds) + 's')