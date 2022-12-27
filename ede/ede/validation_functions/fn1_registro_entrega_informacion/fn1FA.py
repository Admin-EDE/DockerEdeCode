from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.validation_functions.check_utils as check_utils
from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn1FA(conn, return_dict):
    """
    REGISTRO DE ENTREGA DE INFORMACIÓN
    8.0 De la entrega de información
    Cada estudiante tiene al menos una persona autorizada para retirarlo del establecimiento.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos retirados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Alumnos retirados tienen incidente, fecha, registro de entrega de información, 
            firma digital del profesor, documento o firma digital del apoderado
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _Apo = []
    try:

     # VALIDO LA EXISTENCIA DE ALUMNOS RETIRADOS Y QUE TENGAN REGISTRADA FECHA DE RETIRO
        _s1 = """SELECT A.personId,B.Identifier,C.OrganizationPersonRoleId ,C.ExitDate
	FROM PersonStatus A
	JOIN PersonIdentifier B
		ON A.personId = B.personId
		AND A.RecordEndDateTime IS NULL
		AND B.RecordEndDateTime IS NULL
	JOIN OrganizationPersonRole C
		ON A.personId = C.personId
		AND C.RecordEndDateTime IS NULL
	WHERE A.RefPersonStatusTypeId = 30;"""

        # OBTENGO INFORMACION DE APODERADO
        _s2 = """SELECT A.RelatedPersonId ,A.personId
                FROM PersonRelationship A
                JOIN OrganizationPersonRole B
                  ON A.RelatedPersonId = B.personId
                  AND A.RecordEndDateTime IS NULL
		          AND B.RecordEndDateTime IS NULL
                WHERE A.personId = ?
                AND A.RefPersonRelationshipId = 31 --Apoderado
                AND B.RoleId = 15;""" #Padre, madre o apoderado

        # OBTENGO ID DE INCIDENTE ASOCIADO
        _s3 = """SELECT A.IncidentId
                FROM IncidentPerson A
                JOIN Incident B
                ON A.IncidentId = B.IncidentId
                AND A.RecordEndDateTime IS NULL
		          AND B.RecordEndDateTime IS NULL
                WHERE A.personId = ?
                AND B.RefIncidentBehaviorId = 33;""" #Entrega de documentos retiro de un estudiante

        # OBTENGO INFORMACION DE PERSONAS ASOCIADAS A INCIDENTE
        _s4 = """SELECT A.personId,A.RefIncidentPersonTypeId ,A.digitalRandomKey, A.fileScanBase64
                FROM IncidentPerson A
                WHERE A.IncidentId = ?
                AND A.RecordEndDateTime IS NULL;"""

        # VERIFICA SI EXISTE REGISTRO DE RETIROS ANTICIPADOS DEL ESTABLECIMIENTO (OrganizationPersonRole)
        _r = ejecutar_sql(conn, _s1)
        if(len(_r) > 0):
            _p = check_utils.convertirArray2DToList(
                list([m[0] for m in _r if m[0] is not None]))
            _i = check_utils.convertirArray2DToList(
                list([m[1] for m in _r if m[1] is not None]))
            _opr = check_utils.convertirArray2DToList(
                list([m[2] for m in _r if m[2] is not None]))
            _ed = check_utils.convertirArray2DToList(
                list([m[3] for m in _r if m[3] is not None]))
            # VALIDO QUE REIGISTRO DE RETIRO TENGA FECHA DEL EVENTO
            if (len(_p) > len(_ed)):
                logger.error(
                    f"Existen registros de retiros de estudiantes del establecimiento sin fecha de evento.")
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            else:
                for p in _p:
                    _v = str(p)
                    _r2 = ejecutar_sql(conn, _s2, _v)
                    if(len(_r2) > 0):
                        for rp in _r2:
                            _v3 = str(rp[0])
                            _r3 = ejecutar_sql(conn, _s3, _v3)
                            if(len(_r3) > 0):
                                for r3 in _r3:
                                    _v4 = r3
                                    _r4 = ejecutar_sql(conn, _s4, _v4)
                                    if(len(_r4) > 0):
                                        for r4 in _r4:
                                            va1 = str(r4[2])
                                            va2 = str(r4[3])
                                            if(str(r4[1]) == "44"):  # docente
                                                if va1 is None:
                                                    logger.error(
                                                        f"No hay registro de firma digital de docente / administrativo para incidente.")
                                                    logger.error(f"Rechazado")
                                                    return_dict[getframeinfo(
                                                        currentframe()).function] = False
                                                    return False
                                            elif(str(r4[1]) == "43"):  # apoderado
                                                if va1 is None:
                                                    if va2 is None:
                                                        logger.error(
                                                            f"No hay registro de firma digital ni documento digitalizado de apoderado para incidente.")
                                                        logger.error(
                                                            f"Rechazado")
                                                        return_dict[getframeinfo(
                                                            currentframe()).function] = False
                                                        logger.info(f"{current_process().name} finalizando...")
                                                        return False
                                    else:
                                        logger.error(
                                            f"No hay registro de personas asociadas a incidente Id: {str(r3)}")
                                        logger.error(f"Rechazado")
                                        return_dict[getframeinfo(
                                            currentframe()).function] = False
                                        logger.info(f"{current_process().name} finalizando...")
                                        return False

                            else:
                                logger.error(
                                    f"No hay registro de entrega de informacion por retiro de estudiante de establecimiento.")
                                logger.error(f"Rechazado")
                                return_dict[getframeinfo(
                                    currentframe()).function] = False
                                logger.info(f"{current_process().name} finalizando...")
                                return False
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
        else:
            logger.info(
                f"NO existen registros de retiro de alumnos del establecimiento.")
            logger.info(f"S/Datos")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
