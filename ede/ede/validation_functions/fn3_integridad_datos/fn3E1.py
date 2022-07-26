
from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


def fn3E1(conn, return_dict):
    """ 
    Integridad: VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay docentes registrados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los docentes tienen su título y datos de la institución donde estudiaron
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
SELECT DISTINCT
	 pdc.personId 
	 ,pdc.DegreeOrCertificateTitleOrSubject
	 ,RefDegreeOrCertificateType.Description
	 ,pdc.AwardDate
	 ,pdc.NameOfInstitution
	 ,RefHigherEducationInstitutionAccreditationStatus.Description
	 ,RefEducationVerificationMethod.Description
	 ,pdc.idoneidadDocente
	 ,role.Name as 'RoleName'
FROM PersonDegreeOrCertificate pdc
OUTER LEFT JOIN RefDegreeOrCertificateType USING(RefDegreeOrCertificateTypeId)
OUTER LEFT JOIN RefHigherEducationInstitutionAccreditationStatus USING(RefHigherEducationInstitutionAccreditationStatusId)
OUTER LEFT JOIN RefEducationVerificationMethod USING(RefEducationVerificationMethodId)
JOIN OrganizationPersonRole USING(personId)
JOIN Role USING(RoleId)
WHERE RoleName IN ('Director(a)','Jefe(a) UTP','Inspector(a)','Profesor(a) Jefe','Docente','Asistente de la Educación','Técnica(o) de párvulo','Paradocente','Profesor(a) de reemplazo')
    """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        logger.info(f"len(docentes): {len(rows)}")

        if(len(rows) > 0):
            personId = check_utils.convertirArray2DToList(
                list([m[0] for m in rows if m[0] is not None]))
            title = check_utils.convertirArray2DToList(
                list([m[1] for m in rows if m[1] is not None]))
            Type = check_utils.convertirArray2DToList(
                list([m[2] for m in rows if m[2] is not None]))
            AwardDate = check_utils.convertirArray2DToList(
                list([m[3] for m in rows if m[3] is not None]))
            Institution = check_utils.convertirArray2DToList(
                list([m[4] for m in rows if m[4] is not None]))
            AccreditationStatus = check_utils.convertirArray2DToList(
                list([m[5] for m in rows if m[5] is not None]))
            VerificationMethod = check_utils.convertirArray2DToList(
                list([m[6] for m in rows if m[6] is not None]))
            idoneidadDocente = check_utils.convertirArray2DToList(
                list([m[6] for m in rows if m[7] is not None]))
            _r = len(personId) == len(title) == len(Type) == len(AwardDate) == len(Institution) == len(
                AccreditationStatus) == len(VerificationMethod) == len(idoneidadDocente)
            _t = f"VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema: {_r}."
            logger.info(_t) if _r else logger.error(_t)
            logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        else:
            logger.info("S/Datos")
            _r = True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
