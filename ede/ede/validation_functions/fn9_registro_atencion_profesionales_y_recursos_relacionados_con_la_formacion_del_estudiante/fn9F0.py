from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn9F0(conn, return_dict):
    """
    REGISTRO DE ATENCIÓN DE PROFESIONALES Y DE RECURSOS RELACIONADOS CON LA FORMACIÓN DEL ESTUDIANTE
    6.2 Contenido mínimo, letra f
    Verificar que la información del equipo de docentes 
    y profesionales relacionados con la formación del estudiante 
    se encuentren registrados en el sistema.
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
        i = 0
        docentes = conn.execute("""--sql
        SELECT
              P.PersonId,
              PDOC.DegreeOrCertificateTitleOrSubject
        from Organization o
                join OrganizationPersonRole OPR on o.OrganizationId = OPR.OrganizationId
                join Person P on OPR.PersonId = P.PersonId
                join PersonDegreeOrCertificate PDOC on P.PersonId = PDOC.PersonId
        where OPR.RoleId = 5
        group by P.PersonId
        """).fetchall()
        a = len(docentes)
        if(len(docentes)):
            for fila in docentes:
                for column in fila:
                    if column is None:
                        logger.info(f'informacion incompleta del profesor')
                        logger.info(f'Rechazado')
                        return_dict[getframeinfo(
                            currentframe()).function] = False
                        return False
                    else:
                        if i == a:
                            logger.info(f'Informacion de profesores completa')
                            logger.info(f'Aprobado')
                            return_dict[getframeinfo(
                                currentframe()).function] = True
                            return True
                        else:
                            i += 1
        else:
            logger.error(f'S/Datos')
            logger.error(f'Sin datos de docentes')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
