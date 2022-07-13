from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger

def fn29B(conn, return_dict):
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
        query = conn.execute("""
        SELECT OPR.OrganizationId, P.PersonId, count(P.PersonId)
        from Person P
                join OrganizationPersonRole OPR on P.PersonId = OPR.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        where RoleId = 6
          and ps.RefPersonStatusTypeId = 35
          and OrganizationPersonRoleId in (select OrganizationId
                                          from Organization
                                          where RefOrganizationTypeId = 47)
        group by OPR.OrganizationId, P.PersonId;
        """).fetchall()
        k12StudentEnrollment = conn.execute("""
        select OrganizationPersonRoleId
        from K12StudentEnrollment;
        """).fetchall()
        if(len(query)>0 and len(k12StudentEnrollment)>0):
            estudiantes = (list([m[2] for m in query if m[2] is not None]))
            organizaciones = (list([m[0] for m in query if m[0] is not None]))
            organizacionesK12 = (list([m[0] for m in k12StudentEnrollment if m[0] is not None]))
            for x in estudiantes:
                if(x == 2):
                    logger.error(f"Matriculas repetidas")
                    logger.error(f"Rechazado")
                    return_dict[getframeinfo(currentframe()).function] = False
                    return False
                else:
                    for y in organizacionesK12:
                        for z in organizaciones:
                            if(y in z):
                                contador = contador + 1
                            else:
                                logger.error(f"Matricula/s no registrada/s")
                                logger.error(f"Rechazado")
                                return_dict[getframeinfo(currentframe()).function] = False
                                return False
            logger.info(f'Matriculas ingresadas correctamente')
            logger.info(f'Aprobado')
            return_dict[getframeinfo(currentframe()).function] = True
            return True
        else:
            logger.info(f"S/Datos")
            logger.info(f"No existen alumnos en practica registrados")
            return_dict[getframeinfo(currentframe()).function] = True
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
  ## Fin fn29B WC ##