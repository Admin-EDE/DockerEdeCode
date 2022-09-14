from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn6D0(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.3
    Las bajas y altas realizadas en el transcurso del periodo escolar son identificadas y establecida su fecha como insumo para otras verificaciones.
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
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
        SELECT 
          opr.OrganizationPersonRoleId
          ,pid.Identifier
          ,strftime('%Y-%m-%d', opr.EntryDate) as 'EntryDate'
          ,strftime('%Y-%m-%d', opr.ExitDate) as 'ExitDate'
          ,strftime('%Y-%m-%d', pst.StatusStartDate) as 'StatusStartDate'
          ,strftime('%Y-%m-%d', pst.StatusEndDate) as 'StatusEndDate'
        FROM OrganizationPersonRole opr
          JOIN Organization org 
            ON opr.OrganizationId = org.OrganizationId
            AND org.RefOrganizationTypeId IN (
              SELECT RefOrganizationTypeId
              FROM RefOrganizationType
              WHERE RefOrganizationType.description IN ('K12 School')
            )
          JOIN PersonIdentifier pid 
            ON opr.personId = pid.personId
            AND pid.RecordEndDateTime IS NULL
          JOIN RefPersonIdentificationSystem rpis 
            ON pid.RefPersonIdentificationSystemId = rpis.RefPersonIdentificationSystemId
            AND rpis.description In ('ROL UNICO NACIONAL')
          OUTER LEFT JOIN PersonStatus pst 
            ON opr.personId = pst.personId
            AND pst.RecordEndDateTime IS NULL
        WHERE 
          opr.RoleId IN (
            SELECT RoleId
            FROM Role
            WHERE role.Name IN ('Estudiante')
          )
          AND 
          pst.RefPersonStatusTypeId IN (
            SELECT RefPersonStatusTypeId
            FROM RefPersonStatusType
            WHERE RefPersonStatusType.Description IN ('Estudiante retirado definitivamente')
          )                          
      """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    if(len(rows) <= 0):
        _r = True
        logger.info(
            f"No hay registros de alta/baja de alumnos en el establecimiento.")
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        _l = []
        _l2 = []
        for q1 in rows:
            _r = str(q1[1])
            _entryDate = str(q1[2])  # rescata OrganizationPersonRole.entryDate
            _exitDate = str(q1[3])  # rescata OrganizationPersonRole.exitDate
            # rescata personStatus.statusStartDate
            _statusStartDate = str(q1[4])
            _statusEndDate = str(q1[5])  # rescata personStatus.statusEndDate

            if(_entryDate is None) or (_statusStartDate is None):
                _l.append(_r)
            elif(_entryDate != _statusStartDate):
                _l2.append(_r)

            if(_exitDate is None) or (_statusEndDate is None):
                _l.append(_r)
            elif(_exitDate != _statusEndDate):
                _l2.append(_r)

        if(len(_l) != 0):
            logger.error(
                f"Hay alumnos sin rergistro de fecha de alta/baja: {str(_l)}")
            logger.error(f"Rechazado")
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False

        if(len(_l2) != 0):
            logger.error(
                f"Hay alumnos con inconsistencia en registros de alta/baja: {str(_l2)}")
            logger.error(f"Rechazado")
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False

        logger.info(f"Aprobado")
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
