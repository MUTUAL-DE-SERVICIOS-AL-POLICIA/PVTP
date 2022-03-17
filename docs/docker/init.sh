cd /var/www/html/public 
composer run-script post-root-package-install 
composer install 
yes|composer run-script post-create-project-cmd
composer clear-cache
yarn prod
composer run-script post-autoload-dump
chown www-data -R /var/www/html/public
service nginx restart
service redis-server start

