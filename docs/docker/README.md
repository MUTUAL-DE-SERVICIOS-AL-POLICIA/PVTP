# DOCKERIZANDO EL PROYECTO

## Requisitos

* [Docker](https://docs.docker.com/install/)
* [Docker Compose](https://docs.docker.com/compose/install/)
* Conexión a internet

## Configuración

* Una vez clonado el proyecto, dentro del directorio raíz clonar el submódulo Laradock:

```sh
git clone https://github.com/laradock/laradock.git
```

* Copiar las recetas modificadas:

```sh
cp -f docs/docker/docker-compose.yml laradock/
cp -f docs/docker/env-example laradock/.env
```

* Copiar el archivo Dockerfile en la carpeta php-fpm de laradock:

```sh
cp -f docs/docker/Dockerfile laradock/php-fpm/
```

* Ingresar al directorio laradock:

```sh
cd laradock
```
* Verificar que se copiaron los archivos 

```sh
laradock/docker-compose.yml
laradock/.env
```

* Modificar el archivo `.env` de laradock de acuerdo a los puertos que se irán a utilizar.

```sh
NGINX_HOST_HTTP_PORT=80
```

* Construir las imagenes:

```sh
docker-compose build --no-cache nginx php-fpm workspace laravel-echo-server redis
```
* Levantar los contenedores:

```sh
docker-compose up -d nginx php-fpm workspace laravel-echo-server redis
```

## Instalar las dependencias

* Verificar que los contenedores se encuentren funcionando:

```sh
docker-compose ps -a
```
```sh
loans_laravel-echo-server_1
loans_nginx_1
loans_php-fpm_1
loans_redis_1
loans_workspace_1
```

* Instalar las fuentes dentro del contenedor `php-fpm`:

```sh
docker-compose exec php-fpm /var/www/install-roboto-fonts.sh
```

* Instalar el soporte para lenguaje español:

```sh
docker-compose exec php-fpm /var/www/install-spanish-locale.sh
docker-compose exec workspace /var/www/install-spanish-locale.sh
```

* Dentro del contenedor workspace, generar las variables de entorno:

```sh
docker-compose exec --user laradock workspace composer run-script post-root-package-install
```
* Instalar las dependencias del proyecto dentro los contenedores:

```sh
docker-compose exec --user laradock workspace composer install
```

* Generar las llaves de sesión y de autenticación:

```sh
docker-compose exec --user laradock workspace composer run-script post-create-project-cmd
```

* Cambiar el modo de laravel-echo-server a producción

```sh
docker-compose exec laravel-echo-server sed -i 's/\"devMode\":.*/\"devMode\": false,/g' laravel-echo-server.json
docker-compose restart laravel-echo-server
```

* Modificar el archivo `.env` de laravel de acuerdo a las credenciales de base de datos, sockets, etc.

* Compilar el codigo para producción

```sh
docker-compose exec --user laradock workspace yarn prod
```

## Para continuar con el desarrollo

* Cambiar la variable APP_ENV=development en el archivo `.env` de laravel y compilar el código:

```sh
docker-compose exec --user laradock workspace yarn dev
```

## Problemas comúnes

* Si se encuentran problemas en la console de error, verificar los contenedores en estado `exited`:

```sh
docker-compose ps
```

* Se pueden verificar los log's de los contenedores levantados o hacer seguimiento en caso de que algun contenedor genere algun error

```sh
docker-compose logs nginx

docker-compose -f nginx
```

* Eliminar los contenedores que no se encuentren levantados:

```sh
docker rm every_unused_container
```

* En caso de seguir con problemas, eliminar todos los contenedores, imágenes, redes y volúmenes que no se encuentren en uso:

```sh
docker container prune
docker image prune -a
docker network prune
docker volume prune
```

* Volver a generar las imágenes desde cero.
