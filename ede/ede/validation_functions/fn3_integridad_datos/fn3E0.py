from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger

### INICIO fn3E0 ###
def fn3E0(conn, return_dict):
    """
    Integridad: VERIFICA SI LA VISTA PersonList filtrada por docentes contiene información
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - encuentra información en la vista
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
      rows = conn.execute("""
      SELECT
        personId
        ,DegreeOrCertificateTitleOrSubject
        ,DegreeOrCertificateTypeDescription
        ,AwardDate
        ,NameOfInstitution
        ,higherEducationInstitutionAccreditationStatusDescription
        ,educationVerificationMethodDescription
      FROM PersonList
      WHERE Role like '%Docente%';
    """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
      logger.info(f"len(docentes): {len(rows)}")

      if( len( rows ) > 0 ):
        logger.info(f"Aprobado")
        _r = True
      else:
        logger.info(f"S/Datos")
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")        
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r
  ### FIN fn3E0 ###