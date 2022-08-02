from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn2AA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.6 De los estudiantes de intercambio
    Validar que exista cargada en el sistema la resolución que autoriza al estudiante.
    --------------------------------------------------
    NOTA: File 65 sólo indicaba que esta verificación es complementaria a otra existente. Se ajustó comentario.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos de intercambio registrados en el establecimiento
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - todos los alumnos de intercambios fueron aprobados
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        results = conn.execute("""--sql
        SELECT p.personId
        FROM Person p
                JOIN PersonStatus ps on p.PersonId = ps.PersonId
        WHERE p.RefVisaTypeId = 6 --Exchange Scholar Visa
          AND ps.StatusValue = 1 --aprobado o no
          AND ps.RefPersonStatusTypeId = 25; --Intercambio
        """)
        if not results.returns_rows:
          logger.info(
                f"No existen estudiantes intercambio registrados en el establecimiento")
          logger.info(f"S/Datos")
          return_dict[getframeinfo(currentframe()).function] = True
          logger.info(f"{current_process().name} finalizando...")
          return True
        results = results.fetchall()

        resultsTwo = conn.execute("""--sql
        SELECT p.personId
        FROM Person p
                JOIN PersonStatus ps on p.PersonId = ps.PersonId
        WHERE p.RefVisaTypeId = 6 --Exchange Scholar Visa
          and ps.RefPersonStatusTypeId = 25; --Intercambio
        """)
        if not resultsTwo.returns_rows:
          logger.info(
                f"No existen estudiantes intercambio registrados en el establecimiento")
          logger.info(f"S/Datos")
          return_dict[getframeinfo(currentframe()).function] = True
          logger.info(f"{current_process().name} finalizando...")
          return True
        resultsTwo = resultsTwo.fetchall()

        if(len(results) > 0 and len(resultsTwo) > 0):
            lista = list(set([m[0] for m in results if m[0] is not None]))
            listaDos = list(
                set([m[0] for m in resultsTwo if m[0] is not None]))

            if lista == listaDos:
                logger.info(
                    f"todos los alumnos de intercambios fueron aprobados")
                logger.info(f"Aprobado")
                return_dict[getframeinfo(currentframe()).function] = True
                logger.info(f"{current_process().name} finalizando...")
                return True
            else:
                logger.error(
                    f'No todos los alumnos de intercambio han sido aprobados')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.info(f"S/Datos")
            logger.info(
                f"No hay alumnos de intercambio registrados en el establecimiento")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
