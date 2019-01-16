# fusedrive

A simple and fast way to mount Google Drive over fuse.

## Installation

First enable the Google Drive API in your Google Cloud Platform console. You can do that here: https://developers.google.com/drive/api/v3/quickstart/go

It will provide you with a credentials.json file that will be used to authenticate to your Google Drive.

Now run:
```bash
cp ~/Downloads/credentials.json /home/core/fusedrive/
docker run -it \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  -v /media/drive:/media/drive:shared \
  -v /home/core/fusedrive:/var/fusedrive \
  fusedrive
```
