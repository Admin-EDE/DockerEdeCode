from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn2AA(conn, return_dict):
    """ Breve descripción de la función
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - A
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        results = conn.execute("""
        SELECT p.personId
        FROM Person p
                JOIN PersonStatus ps on p.PersonId = ps.PersonId
        WHERE p.RefVisaTypeId = 6
          AND ps.StatusValue = 1
          AND ps.RefPersonStatusTypeId = 25;
        """).fetchall()

        resultsTwo = conn.execute("""
        SELECT p.personId
        FROM Person p
                JOIN PersonStatus ps on p.PersonId = ps.PersonId
        WHERE p.RefVisaTypeId = 6
          and ps.RefPersonStatusTypeId = 25;
        """).fetchall()

        if(len(results) > 0 and len(resultsTwo) > 0):
            lista = list(set([m[0] for m in results if m[0] is not None]))
            listaDos = list(
                set([m[0] for m in resultsTwo if m[0] is not None]))

            if lista == listaDos:
                logger.info(
                    f"todos los alumnos de intercambios fueron aprobados")
                logger.info(f"Aprobado")
                return_dict[getframeinfo(currentframe()).function] = True
                return True
            else:
                logger.error(
                    f'No todos los alumnos de intercambio han sido aprobados')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                return False
        else:
            logger.info(f"S/Datos")
            logger.info(
                f"No hay alumnos de intercambio registrados en el establecimiento")
            return_dict[getframeinfo(currentframe()).function] = True
            return True

    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
