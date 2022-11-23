from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn29C(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.7 De los estudiantes en práctica
    Los estudiantes egresados de cuarto medio y que estén realizando su práctica tienen asignado un profesor tutor.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos en práctica
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los alumnos en práctica tienen un tutor asignado
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _queryStud = ejecutar_sql(conn, """--sql
        SELECT OPR.OrganizationId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join Organization O on OPR.OrganizationId = O.OrganizationId
        WHERE OPR.RoleId = 6 --Estudiante
          AND O.RefOrganizationTypeId = 47 --Asignatura de practica profesional
        GROUP by P.PersonId,
                OPR.OrganizationId;
        """)

        _queryProf = ejecutar_sql(conn, """--sql
        SELECT OPR.OrganizationId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join Organization O on OPR.OrganizationId = O.OrganizationId
        WHERE OPR.RoleId = 17 --Tutor(a) de practica profesional
          AND O.RefOrganizationTypeId = 47 --Asignatura de practica profesional
        GROUP by P.PersonId,
                OPR.OrganizationId;
        """)
        if((len(_queryStud) > 0) and (len(_queryProf) > 0)):
            _organizationStu = (
                list([m[0] for m in _queryStud if m[0] is not None]))
            if not _organizationStu:
                logger.error(f"Sin Alumnos")
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            _organizationProf = (
                list([m[0] for m in _queryProf if m[0] is not None]))
            if not _organizationProf:
                logger.error(f"Sin profesores")
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            _contador = 0
            z = len(_organizationStu)
            for x in _organizationStu:
                for y in _organizationProf:
                    if x == y:
                        _contador += 1
            if _contador == z:
                logger.info(f'Todos los alumnos en practica con profesor')
                logger.info(f'Aprobado')
                return_dict[getframeinfo(currentframe()).function] = True
                logger.info(f"{current_process().name} finalizando...")
                return True
            else:
                logger.error(f'Alumnos en practica sin profesor')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.info(f"S/Datos")
            logger.info(f"No existen alumnos en practica registrados")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
