from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn29B(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.7 De los estudiantes en práctica
    Si la práctica se realiza durante la jornada escolar o en vacaciones existe solo un registro de matricula para el estudiante.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos con formación dual
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay alumnos con formación dual y tienen su matrícula no repetida y registrada correctamente
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        query = ejecutar_sql(conn, """--sql
        SELECT OPR.OrganizationPersonRoleId, P.PersonId, count(P.PersonId)
        from Person P
                join OrganizationPersonRole OPR on P.PersonId = OPR.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        where RoleId = 6 --Estudiante
          and ps.RefPersonStatusTypeId = 35 --Estudiante con formacion dual
          and OPR.OrganizationId in (select OrganizationId
                                          from Organization
                                          where RefOrganizationTypeId = 47) --Asignatura de Práctica
        group by OPR.OrganizationId, P.PersonId;
        """)
        k12StudentEnrollment = ejecutar_sql(conn, """--sql
        select K12.OrganizationPersonRoleId
        from K12StudentEnrollment K12
        join OrganizationPersonRole OPR on OPR.OrganizationPersonRoleId = K12.OrganizationPersonRoleId
        join Organization ORG on ORG.OrganizationId = OPR.OrganizationId
        where ORG.RefOrganizationTypeId = 47;
        """)
        if(len(query) > 0 and len(k12StudentEnrollment) > 0):
            estudiantes = (list([m[2] for m in query if m[2] is not None]))
            organizaciones = (list([m[0] for m in query if m[0] is not None]))
            organizacionesK12 = (
                list([m[0] for m in k12StudentEnrollment if m[0] is not None]))
            contador = 0
            for x in estudiantes:
                if(x == 2):
                    logger.error(f"Matriculas repetidas")
                    logger.error(f"S/Datos") #Rechazado, se cambia por S/Datos por error en la consulta
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
                else:
                    for y in organizacionesK12:
                        for z in organizaciones:
                            if(y == z):
                                contador = contador + 1
                            else:
                                logger.error(f"Matricula/s no registrada/s")
                                logger.error(f"S/Datos") #Rechazado, se cambia por S/Datos por error en la consulta
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
            logger.info(f'Matriculas ingresadas correctamente')
            logger.info(f'Aprobado')
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
        else:
            logger.info(f"S/Datos")
            logger.info(f"No existen alumnos en practica registrados")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"S/Datos") #Rechazado, se cambia por S/Datos por error en la consulta
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
