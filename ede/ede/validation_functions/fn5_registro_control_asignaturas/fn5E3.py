from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import sys


from ede.ede._logger import logger

def fn5E3(conn, return_dict):
    """ 
    6.2 Contenido mínimo, letra b.2
    Validar que la clase con reemplazante no idóneo no sea contabilizada 
    en el cumplimiento del plan de estudio y sea considerada al momento de presentar
    un calendario de recuperación.
    
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
    _r = True
    suplencias_noidoneas = []
    try:
        i=0
        suplencias_noidoneas = conn.execute("""
            SELECT
              FirstName,
              MiddleName,
              LastName,
              OCS.claseRecuperadaId,
              digitalRandomKey
            FROM Organization o
            JOIN OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
            JOIN RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
            JOIN Person P on OPR.PersonId = P.PersonId
            JOIN PersonDegreeOrCertificate PDOC on P.PersonId = PDOC.PersonId
            JOIN OrganizationCalendar OC on o.OrganizationId = OC.OrganizationId
            JOIN OrganizationCalendarSession OCS on OC.OrganizationCalendarId = OCS.OrganizationCalendarId
            JOIN CourseSection CS on o.OrganizationId = CS.OrganizationId
            JOIN CourseSectionLocation CSL on CS.OrganizationId = CSL.OrganizationId
            JOIN Classroom Cr on CSL.LocationId = Cr.LocationId
            JOIN LocationAddress L on  L.LocationId = Cr.LocationId
            where OPR.RoleId !=6
            and PDOC.idoneidadDocente != 1
            and LOWER(RAE.observaciones) like '%falta docente%';
        """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {suplencias_noidoneas} -> {str(e)}")

    if(len(suplencias_noidoneas)<=0):
      _r = True
      logger.info(f"S/Datos")
      return_dict[getframeinfo(currentframe()).function] = _r
      return _r
        
    try:
        a=len(suplencias_noidoneas)
        for fila in suplencias_noidoneas:
          if fila[3] is None or 0 or fila[4] is None  :
            logger.error(f'clase con profesor suplente no idoneo, no registrada para recuperar o registrada pero no firmada')
          else:
            if i == a and fila[a][3] is not None or 0 and fila[a][4] is not None:
              logger.info(f'verificacion aprobada,Todas las suplencias tienen indicada la recuperacion y estan firmadas')
              _r = True              
            else:
              i+=1
    except Exception as e:
      logger.error(f"Error on line {sys.exc_info()[-1].tb_lineno}, {type(e).__name__},{e}")
      logger.error(f"{str(e)}")
    finally:
      logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r
  ## fin fn5E3 WC ##