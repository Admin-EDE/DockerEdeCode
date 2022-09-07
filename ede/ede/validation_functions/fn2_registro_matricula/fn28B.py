from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn28B(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.8 De los estudiantes migrantes
    Validar que el certificado de convalidación de estudios y
    los cursos convalidados se encuentre cargado en el sistema.
    --------------------------------------------------
    Este tipo de casos se registrará a través de la tabla 
    PersonStatus.refPersonStatusTypeId == 34 (Convalidación de estudios) y 
    los campos personStatus.docNumber, personStatus.Description y 
    personStatus.fileScanBase64 se utilizanrán para almacenar la información 
    de respaldo de este proceso extraordinario.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay estudiantes migrantes
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay estudiantes migrantes y estos tienen documento de convalidación de asignaturas
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        --Busca estudiantes migrantes
        SELECT DISTINCT PI.PersonId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonIdentifier PI on P.PersonId = PI.PersonId
        WHERE PI.RefPersonIdentificationSystemId = 52 --Identificador provisorio escolar (IPE)
          AND OPR.RoleId = 6 --Estudiante
          AND PI.Identifier is not null;
        """)
        if(len(_query) > 0):
            _queryDocuments = ejecutar_sql(conn, """--sql
          SELECT PS.fileScanBase64
          FROM PersonStatus PS
          WHERE PS.PersonId in (select DISTINCT PI.PersonId
                                from OrganizationPersonRole OPR
                                        join Person P on OPR.PersonId = P.PersonId
                                        join PersonIdentifier PI on P.PersonId = PI.PersonId
                                where PI.RefPersonIdentificationSystemId = 52
                                  and OPR.RoleId = 6 --Estudiante
                                  and PI.Identifier is not null)
            AND PS.docNumber IS NOT NULL
            AND PS.docNumber <> ''
            AND PS.Description IS NOT NULL
            AND PS.Description <> ''
            and PS.fileScanBase64 is not null
            and PS.RefPersonStatusTypeId = 34 --Convalidacion de estudios
          """)
            if (len(_queryDocuments) == len(_query)):
                _file = ejecutar_sql(conn, """--sql
                --Busca documentos donde los estudiantes sean migrantes
            SELECT documentId
            FROM Document
            WHERE fileScanBase64 IS NOT NULL
              AND fileScanBase64 <> ''
              AND documentId in (SELECT PS.fileScanBase64
                                FROM PersonStatus PS
                                WHERE PS.PersonId in (select DISTINCT PI.PersonId
                                                      from OrganizationPersonRole OPR
                                                                join Person P on OPR.PersonId = P.PersonId
                                                                join PersonIdentifier PI on P.PersonId = PI.PersonId
                                                      where PI.RefPersonIdentificationSystemId = 52 --Identificador provisorio escolar (IPE)
                                                        and OPR.RoleId = 6 --Estudiante
                                                        and PI.Identifier is not null)
                                  AND PS.docNumber IS NOT NULL
                                  AND PS.docNumber <> ''
                                  AND PS.Description IS NOT NULL
                                  AND PS.Description <> ''
                                  and PS.fileScanBase64 is not null
                                  and PS.RefPersonStatusTypeId = 34); --Convalidacion de estudios
            """)
                if(len(_file) == len(_query)):
                    logger.info(
                        f'Todos los estudiantes migrantes cuentan con sus documentos de convalidacion de ramos completos')
                    logger.info(f'Aprobado')
                    return_dict[getframeinfo(currentframe()).function] = True
                    logger.info(f"{current_process().name} finalizando...")
                    return True
                else:
                    logger.error(
                        f'Existen alumnos migrantes con documentos de convalidacion de ramos incompletos')
                    logger.error(f'Rechazado')
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
            else:
                logger.error(
                    f'Existen alumnos migrantes con documentos de convalidacion de ramos incompletos')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.info(
                f"No existen estudiantes migrantes registrados en el establecimiento")
            logger.info(f"S/Datos")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
