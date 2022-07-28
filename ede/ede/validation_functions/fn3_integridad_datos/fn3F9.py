from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

def fn3F9(conn, return_dict):
    """
    Integridad: Verifica que las fechas ingresadas cumplan con el formato
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verifica que el campo cumpla con la siguiente expresión regular:
^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))[ T]?((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.\d{0,})?)?([+-](0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))?$
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    shortDateAllRecords = []
    try:
      shortDateQuery = """
          SELECT DISTINCT shortDate
          FROM (
            SELECT StartDate as shortDate
            FROM organizationCalendarCrisis
            UNION ALL
            SELECT EndDate
            FROM organizationCalendarCrisis
            UNION ALL
            SELECT CrisisEndDate
            FROM organizationCalendarCrisis
            UNION ALL
            SELECT Birthdate
            FROM Person
            UNION ALL
            SELECT AwardDate
            FROM PersonDegreeOrCertificate
            UNION ALL
            SELECT IncidentDate
            FROM Incident
            UNION ALL
            SELECT Date
            FROM IncidentPerson
            UNION ALL
            SELECT DisciplinaryActionStartDate
            FROM K12StudentDiscipline
            UNION ALL
            SELECT DisciplinaryActionEndDate
            FROM K12StudentDiscipline
            UNION ALL
            SELECT StatusStartDate
            FROM PersonStatus
            UNION ALL
            SELECT StatusEndDate
            FROM PersonStatus
            UNION ALL
            SELECT StatusStartDate
            FROM RoleStatus
            UNION ALL
            SELECT StatusEndDate
            FROM RoleStatus
            UNION ALL
            SELECT rexDate
            FROM OrganizationCalendarEvent
            UNION ALL
            SELECT BeginDate
            FROM OrganizationCalendarSession
            UNION ALL
            SELECT EndDate
            FROM OrganizationCalendarSession
            UNION ALL
            SELECT FirstInstructionDate
            FROM OrganizationCalendarSession
            UNION ALL
            SELECT LastInstructionDate
            FROM OrganizationCalendarSession
          )
          WHERE 
            shortDate IS NOT NULL
      """
      shortDateAllRecords = conn.execute(shortDateQuery).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {shortDateAllRecords} -> {str(e)}")
    
    try:
      shortDateQueryWithRegexp = shortDateQuery + """ AND shortDate NOT REGEXP "^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))$" """
      shortDateDataWithErrors = conn.execute(shortDateQueryWithRegexp).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {shortDateDataWithErrors} -> {str(e)}")

    fullDateTimeAllRecords = []
    try:
      fullDateTimeQuery ="""
          SELECT DISTINCT fullDateTime
          FROM (
            SELECT Date as fullDateTime
            FROM RoleAttendanceEvent
            UNION ALL
            SELECT digitalRandomKeyDate as fullDateTime
            FROM RoleAttendanceEvent
            UNION ALL
            SELECT EntryDate
            FROM OrganizationPersonRole
            UNION ALL
            SELECT ExitDate
            FROM OrganizationPersonRole	
          )
          WHERE 
            fullDateTime IS NOT NULL
      """
      fullDateTimeAllRecords = conn.execute(fullDateTimeQuery).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {fullDateTimeAllRecords} -> {str(e)}")
      
    try:
      fullDateTimeQueryWithRegexp = fullDateTimeQuery + """ AND fullDateTime NOT REGEXP "^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))[ T]?((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.\d{0,})?)([+-](0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$" """      
      fullDateTimeDataWithErrors = conn.execute(fullDateTimeQueryWithRegexp).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {fullDateTimeDataWithErrors} -> {str(e)}")
    
    try:
      shortDateAllData = check_utils.convertirArray2DToList(list([m[0] for m in shortDateAllRecords if m[0] is not None])) # Valida lista de rut ingresados a la BD 
      fullDateTimeAllData = check_utils.convertirArray2DToList(list([m[0] for m in fullDateTimeAllRecords if m[0] is not None])) # Valida lista de rut ingresados a la BD       
      
      if(len(shortDateAllData) == 0 and len(fullDateTimeAllData) == 0):
        logger.info("S/Datos")
      else:     
        shortDateData = check_utils.convertirArray2DToList(list([m[0] for m in shortDateDataWithErrors if m[0] is not None])) # Valida lista de rut ingresados a la BD 
        fullDateTimeData = check_utils.convertirArray2DToList(list([m[0] for m in fullDateTimeDataWithErrors if m[0] is not None])) # Valida lista de rut ingresados a la BD       

        if(len(shortDateData) == 0 and len(fullDateTimeData) == 0):
          logger.info("Aprobado")
          _r = True
        elif( len(shortDateData) >= 0 or len(fullDateTimeData) >= 0):
          logger.error(f"Rechazado")
          logger.error(f"shortDateData: {set(shortDateData)}")
          logger.error(f"fullDateTimeData: {set(fullDateTimeData)}")
        
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")
      return _r