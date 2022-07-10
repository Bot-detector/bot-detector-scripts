#!/bin/sh

sudo mysql -u root </home/ubuntu/000_recreate_playerdata_dev.sql
sudo mysqldump -u root --no-data --routines --events playerdata | sudo mysql -u root playerdata_dev
sudo mysql -u root playerdata_dev </home/ubuntu/001_insert_apiUser.sql
sudo mysql -u root playerdata_dev </home/ubuntu/002_insert_players.sql
sudo mysql -u root playerdata_dev </home/ubuntu/003_insert_playerdata_dev.sql
sudo mysql -u root playerdata_dev </home/ubuntu/004_insert_playerHiscoreData.sql
sudo mysql -u root playerdata_dev </home/ubuntu/005_insert_reports.sql
sudo mysqldump -u root --routines --events --databases playerdata_dev | sed 's/playerdata_dev/playerdata/g'|  gzip >/home/ubuntu/100_playerdata_dev.sql.gz

# sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerReports.sql
# sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerReportsManual.sql
# sudo mysql -u root playerdata_dev < /home/ubuntu/insert_playerLocationsDetail.sql
