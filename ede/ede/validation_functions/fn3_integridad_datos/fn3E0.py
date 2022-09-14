from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3E0(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Hay docentes registrados (vista personList filtrada por docente)
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
        rows = ejecutar_sql(conn, """--sql
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
    """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        logger.info(f"len(docentes): {len(rows)}")

        if(len(rows) > 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"S/Datos")
    except Exception as e:
        logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
