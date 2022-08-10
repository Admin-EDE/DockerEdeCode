# DockerEdeCode
Código utilizado para ejecutar edemineduc/etl

Permite a los proveedores de los LCD encriptar su base de datos y convertir sus datos al formato SQLite necesario para luego subirlos al validador del estándar de datos.
Las instrucciones para descargar, desplegar y ejecutar los scripts del contenedor se encuentran en <https://www.ede.mineduc.cl/desarrolladores/contenedor>. Puede encontrar un diagrama de clases [Aquí](https://www.plantuml.com/plantuml/svg/rLXjRzks4t-lJo7qJHt0CVhm_w_Ri0vzS3gWSzEipeg-8yxaIUPAaIfvSfns-UuZAIPoQSTmXWx3WXYYTB-xuztzZgJqp15OwXfLW5Bcef5hgUj2OKKbcTBAUacba1OvIQCBhi2viWNhyENyFPcJwKdvfIY_D4XB8wvAXzIrZ7UEJCEKgMkq8mqDZajYZg1fZpR0ZH6TGbPPZwsamb5wq2k49wm5Mdwb0bAFZegheZ9Ae2rHOF2_SGSkN1w5fsMqKTuxdSwFts655ZN7uzKJzd2oLcvzBs2rS0Fm7TRIaGrHJRXRHO35UW1m5ExOgqGZ33MtbosXc3wUY4L41m0JYw3HfuelaN-S_pQJX2bNuUzB9L7vSFeCbK_ZGEfwKYFD-gpTjyRG_QCUF-JtavA43BL78-D6Q-Gq4WjePReBPHgNmvXPvCQAtD0Ab1H0-7h-zdJqXpDw2WItUBDOuxeT-PyTbvAmsOlpbrO1nr50XZ9gCIx3iwXrbQMcpyXWYZ6f9J4sSgYgSGcsTaTV8t2Dl2ECW4p4FHpicG8jbOANHi-a8t1dOE5zZDVkqghqurSFeg3FMvWjTLOp8JbbsCbaKkHX6EsCmZUekn3CtQkxE_PCblTAFz_T3PDdHI7eQMnXOozJe283TMrWm-rP3os_HoorGqktosEjS_6GnAGVbvI-MUwSoczEenVWS8fJ8ECYg4_mAJIOmMkaiswX9FyLBrzPqvmq85MlWc6OEzRSofQyWscoEtt1Jf-_ESbmQNKvnpNzxdp8SuatRf8YhUasuoyLh30GVmRphaKhDQ4MNYnXLyY1wgo2JOPOS-6yYqK-VxAWNONM5-iy58jTI5gUWHRWacRW82F3XBICkrNawqsKuz8XFrk2mvm3WM9nFIBZRV-x1CZMaSY2AQetHg0okrGiWdYTbj7UPyf2YbwRpxBDT1IYFJV2k9bnQUi9NcPm3Wi5KtG2drk-b2iJBFTMhvVgCIfiyeYKRAG6E-rQ9Jc8Q-8sbqewocEq2A66AK71xgl4xyJqE0fJIKDkLDV49NLSTbf-wZ3jBMvd9J_CvX_-gvKyMNDKjvHoLoLhf7TOlVM-jFmC98q-lsntJyWqBLWCMUnjguTZzIW3DK1ysM-dFKWRWMCWiWSow1VRjUOChJFwkYj5zK6vo9fexIw6hkFYqcZSChUVLz5X6UgQaIJjE85phlps57bwQZGUpXDXBhGo8C9qaXKPRx9L_seu2uQUwayL9FkU74wLGRQwaGal6-9En-BXmG1aLQTv6BYm5eF004fdsrsebBOM-4UeiQpq0sPJyPYBUCSiahJ6PS270n0r-J4mgIjZcrZa3Fne03T0Ll9EGGPvxDK9Qoi522Q0jXp-Rx017ibHeByuLgD62ygF8AmknZ7UMMSoxVyFsjneVr4CdAmrK6SXT_NTDhX1xJfG6V390CILTvy8pbmyAVa1Yeu3oTLMDDzlHcCeAGPtAxcAcon3nuq2orfZ_PWw8KFScPqVDkmGwc26jTPKwFmePkNI8s96GYLLT0J2H5TC0Rif6HEeCZzNXV-_Aehi5UIdFu-Fqzdxnp8a2BKKKABkWUNnySzxALdi4KRZ-tbuWFP-SXvWOJTZ3z3Rfl4XMdVazY4cznF-00lxkk00xJkqnW7c_dw_763y7soYOdEFYetXFVckKOX5TiO9GYrS4cP7sKow_R8JvEdJmQsOyiOzEcxb0eT-tR7V93F5vjjfG6v4bCkoOzjkSGXgjtXhnZaY-nPI55z_Heg8GQCedg4MuRlKNm00)


---

## Tutorial Ejecución Contenedor
Pasos para ejecutar el código:

1. Instalar Docker desde su [https://www.docker.com/get-started/](Sitio web). Descarga recomendada: Docker Desktop. Requerimientos mínimos: Linux, Windows 10 o superior, Mac OSX

2. Reiniciar el computador

3. Si usas Windows, instalar WSL2 (Windows subsystem for linux), esto para Windows 10 o superior

4. Reiniciar el computador

5. Abrir Docker Desktop, Si se requiere, darle permisos de administrador para que se inicie el servicio (en linux, simplemente inicie el servicio usando systemctl u otro)

6. Abrir una terminal de PowerShell o Terminal.

*Consejo: Apretar Click derecho + Mayus en cualquier carpeta, y aparecerá la opción de abrir PowerShell.*
7. Clonar (o descargar y descomprimir) Repositorio de GitHub: <https://github.com/Admin-EDE/DockerEdeCode>

8. Descargar la imagen del contenedor:
~~~
docker pull edemineduc/etl
~~~
Si aparece un error indicando que no está en ejecución el servicio, puede ser un error en el paso 3 o 4, reiniciar el pc, ejecutar el Docker Desktop y ver que está todo bien, ejecutarlo en modo administrador, y si está andando el servicio y da error, revisar que la terminal tenga acceso a ese servicio, que el usuario tenga acceso al servicio.

9. Ejecutar el comando de ayuda
~~~
docker run -it --rm --name etl -v {ruta_full_del_directorio_local}:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py --help
~~~
Donde dice {ruta_full_del_directorio_local} reemplazar con el directorio del repositorio GitHub descargado previamente. En mi caso de ejemplo quedaría:
~~~
docker run -it --rm --name etl -v C:\Users\erick\OneDrive\Documentos\Trabajo\DockerEdeCode:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py --help
~~~
Si hay espacios es posible que escribiendo el directorio entre comillas funcione, aunque no lo he probado
Reemplazar “--help” con el comando que se quiera, como parse, insert, update, check

---
## Ejemplo de ejecución de comandos más utilizados
- El comando **PARSE** transforma el contenido del JSON.zip en varios archivos CSV. Uno por cada tabla
~~~
docker run -it --rm --name etl -v D:\dockerETL:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py parse json .\json.zip
~~~

- El comando **INSERT** lee todos los archivos csv y los inserta en una base de datos encriptada, que solo la Superintendencia de Educación puede leer.
~~~
docker run -it --rm --name etl -v D:\dockerETL:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py insert
~~~

- El comando **CHECK** reporta si la información contenida en la base de datos cumple con las reglas de validación del Estándar de Datos para la Educación.
~~~
docker run -it --rm --name etl -v D:\dockerETL:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py check {frase_secreta} .\file.db
~~~

---
## Explicación de parámetros del comando check
~~~
docker run -it --rm --name etl -v {ruta_full_del_directorio_local}:/usr/src/ede -w /usr/src/ede edemineduc/etl python3 parseCSVtoEDE.py --help
~~~

- **docker**: comando/aplicación a ejecutarse
- **run**: instrucción a ejecutarse, se utiliza para ejecutar un comando en un contenedor (ejecuta o “enciende” la imagen del contenedor usando los parámetros dados, y luego ejecuta la instrucción o comando dado al final si está la opción -it dada) la documentación completa está en <https://docs.docker.com/engine/reference/run/>
La ejecución típica sin ningún parametro de este comando sería ``docker run edemineduc/etl echo "hola mundo"`` Que ejecuta "echo hola mundo" en la consola del contenedor
- **-it**: Son dos parámetros unidos:
    - **-i**: Conecta el Standard input (STDIN, la entrada de la consola) del contenedor, con el STDIN de la terminal donde se está ejecutando ésta instrucción (permite entonces enviar comandos al contenedor)
    - **-t**: Crea una pseudo terminal al interior del contenedor para poder enviar comandos a ésta
    Ambos comandos se suelen usar juntos, por ello existe la opción -it que evita usar “-i -t”
- **--rm**: Cierra y elimina de la memoria RAM el contenedor una vez que se ejecutó el comando dado. Si no se pone ésto, el contenedor queda encendido esperando comandos.
- **–name etl**: identificador del contenedor en la memoria RAM, sirve para enviarle datos mientras está en ejecución o para simplemente saber su nombre, en este caso es para eso, ponerle un nombre y ya, si no se usa este parámetro se le crea un nombre de letras aleatorias
- **-v**: monta una carpeta del computador local (o una ruta en red de un directorio) en el contenedor, esto es para que utiice dicha carpeta como si estuviese dentro del contenedor
- **{ruta_full_del_directorio_local}** : ruta del directorio en el computador local que va a ser usado desde el contenedor
- **:**: Monta la ruta del directorio de la izquierda en el de la derecha
- **/usr/src/ede**: ruta en la cual el contenedor va a “ver” o acceder al directorio usado arriba
- **-w /usr/src/ede**: Directorio de trabajo (cwd o qwd, current workind dir), el directorio en el cual se abrirá la pseudo terminal creada con -t
- **edemineduc/etl**: Nombre de la imagen a utilizar. Recordar que el contenedor se construye (docker build) para crear una imagen que contiene todos los archivos necesarios y que sirve como espacio de almacenamiento virtual, es similar a el archivo de imagen de una máquina virtual. para ver todas las imágenes disponibles en el computador, ejecutar: “docker images”

    Normalmente el usuario de un contenedor realmente sólo tiene acceso a la imagen, y lo que se llamaría estrictamente contenedor es la imagen ejecutándose, mientras que el código más el dockerfile (que tiene las instrucciones para crear la imagen) es de acceso exclusivo de los desarrolladores del proyecto en cuestión (la imagen es el producto “compilado”, el contenedor es cuando está ejecutándose, la instanciación).
- **python3 parseCSVtoEDE.py --help**: comando a ejecutarse en el contenedor
    - **python3**: ejecuta python3
    - **parseCSVtoEDE.py**: el archivo a ejecutarse, que es el archivo de interfaz para ejecutar todo el proyecto
    - **–help**: muestra la ayuda y los comandos disponibles, que son 4 además de la ayuda por ahora.
