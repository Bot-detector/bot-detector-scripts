sudo mysql -u root < /home/ubuntu/recreate_playerdata_dev.sql
sudo mysqldump -u root --no-data --routines --events playerdata | sudo mysql -u root playerdata_dev
sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerdata_dev.sql
sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerReports.sql
sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerReportsManual.sql
sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerLocationsDetail.sql
sudo mysqldump -u root --routines --events playerdata_dev | gzip > /home/ubuntu/playerdata_dev.sql