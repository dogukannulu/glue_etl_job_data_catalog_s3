# Overview

This process will be running inside an EC2 instance. Therefore, no AWS credential is defined or used inside the scripts. If you want to run locally or somewhere else, you have to add AWS credentials to the scripts.

This part of the repo is created for:

- Create a new S3 bucket or return the related logs if already exists
- Upload a CSV file from a URL into the new bucket with predefined object key
- The CSV won't be modified. If modification is required, you can add to the Python script.
- The script will be valid for the region `eu-central-1`, but you can modify the region part if necessary.

## Steps

1. First, we have to run this command to bring the `setup.sh` into the EC2 instance (We have to be located in `/`).
```
sudo curl -O https://raw.githubusercontent.com/dogukannulu/send_data_to_aws_services/main/csv_to_s3/setup.sh
```

2. The shell script runs the Python script with predefined command line arguments. You can modify lines 51-54 in `setup.sh` according to your use case with `sudo vi setup.sh`. To execute the shell script we should run the following command.
```
sudo chmod +x setup.sh
```

3. After modifications (if necessary), we can create the working directory `/project` and execute the shell script.
```
sudo mkdir project
sudo ./setup.sh
```

4. We can monitor the live logs.
```
sudo tail -f /project/upload_csv_to_s3.log
```

## Notes

- All the processes will be running in `/project` directory after executing `setup.sh`
- You can see all the logs in `/project/upload_csv_to_s3.log` 
- zip file includes `csv_to_s3.py` and `requirements.txt`
- Shell script will download necessary packages, libraries first. Then, will install requirements and run the Python script
