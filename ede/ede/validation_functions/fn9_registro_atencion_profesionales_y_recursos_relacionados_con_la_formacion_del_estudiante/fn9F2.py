from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn9F2(conn, return_dict):
    """
    REGISTRO DE ATENCIÓN DE PROFESIONALES Y DE RECURSOS RELACIONADOS CON LA FORMACIÓN DEL ESTUDIANTE
    6.2 Contenido mínimo, letra f
    Verificar que el registro de la implementación y 
    evaluación del proceso formativo del estudiante se encuentre en el sistema.
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
        queryEstudiantes = ejecutar_sql(conn, """--sql
            SELECT DISTINCT o.OrganizationId, o.Name
            FROM Person p
                    join OrganizationPersonRole opr
                          on p.PersonId = opr.PersonId
                    join Organization O on opr.OrganizationId = O.OrganizationId
            WHERE opr.RoleId = 6
              AND O.RefOrganizationTypeId = 21;
              """)

        if (len(queryEstudiantes) > 0):
            organizations = (
                list([m[0] for m in queryEstudiantes if m[0] is not None]))
            organizations = str(organizations)
            organizations = organizations.replace('[', '(')
            organizations = organizations.replace(']', ')')
            querySelect = "select CourseId from CourseSection where CourseId in"
            queryComplete = querySelect+organizations
            try:
                queryAsignaturas = ejecutar_sql(conn, queryComplete)
                if (len(queryAsignaturas) > 0):
                    cursos = (
                        list([m[0] for m in queryAsignaturas if m[0] is not None]))
                    cursos = str(cursos)
                    cursos = cursos.replace('[', '(')
                    cursos = cursos.replace(']', ')')
                    querySelectCalendar = "select * from OrganizationCalendar where OrganizationId in"
                    queryCalendarComplete = querySelectCalendar+cursos
                    try:
                        queryCalendarios = ejecutar_sql(conn, 
                            queryCalendarComplete)
                        if (len(queryCalendarios) > 0):
                            organizationId = (
                                list([m[1] for m in queryCalendarios if m[0] is not None]))
                            calendarCode = (
                                list([m[2] for m in queryCalendarios if m[0] is not None]))
                            calendarDescripction = (
                                list([m[3] for m in queryCalendarios if m[0] is not None]))
                            calendarYear = (
                                list([m[4] for m in queryCalendarios if m[0] is not None]))

                            if not organizationId:
                                logger.error(f"Sin OrganizationId")
                                logger.error(f'Rechazado')
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
                            if not calendarCode:
                                logger.error(f"Sin CalendarCode")
                                logger.error(f'Rechazado')
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
                            if not calendarDescripction:
                                logger.error(f"Sin CaldendarDescription")
                                logger.error(f'Rechazado')
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
                            if not calendarYear:
                                logger.error(f"Sin CalendarYear")
                                logger.error(f'Rechazado')
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
                            logger.info(
                                f'Calendarios ingresados correctamente')
                            logger.info(f'Aprobado')
                            return_dict[getframeinfo(
                                currentframe()).function] = True
                            logger.info(f"{current_process().name} finalizando...")
                            return True
                        else:
                            # logger.info(f"S/Datos")
                            logger.error(f"Rechazado")
                            return_dict[getframeinfo(
                                currentframe()).function] = False
                            logger.info(f"{current_process().name} finalizando...")
                            return False
                    except Exception as e:
                        logger.error(
                            f"No se pudo ejecutar la consulta: {str(e)}")
                        logger.error(f"Rechazado")
                        return_dict[getframeinfo(
                            currentframe()).function] = False
                        logger.info(f"{current_process().name} finalizando...")
                        return False
                else:
                    # logger.info(f"S/Datos")
                    logger.error(f"Rechazado")
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.info(f"S/Datos")
            logger.info(
                f"Sin datos del registro de implementacion y evaluacion del proceso formativo")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
