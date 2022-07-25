from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


def fn8F2(conn, return_dict):
    """ 
      6.2 Contenido mínimo, letra e
      Verificar el contenido de cada registro de convivencia

      Grupo _Incidentes debería contener todo lo necesario.

      Identificación del estudiante
      Identificación del apoderado
      Fecha, asignatura y observación
      Fecha, profesor y datos de la entrevista con el apoderado      

    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger, solo si no encuentra ningún registro
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - 
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    _queryIncident = []
    try:
        _queryIncident = conn.execute("""
        SELECT DISTINCT
			  I.incidentId                                                                         --00
			, I.incidentDate                                                                       --01
			, I.incidentTime                                                                       --02
			, RefIncidentTimeDescriptionCode.Description 'refIncidentTimeDescriptionCode'          --03
			, I.incidentDescription                                                                --04
			, RefIncidentBehavior.Description 'refIncidentBehavior'                                --05
			, RefIncidentInjuryType.Description 'refIncidentInjuryType'                            --06
			, RefWeaponType.Description 'refWeaponType'                                            --07
			, I.incidentCost                                                                       --08
      , 1                                                                                    --09
			, I.organizationPersonRoleId                                                           --10
			, I.incidentReporterId                                                                 --11
			, RefIncidentReporterType.Description 'refIncidentReporterType'                        --12
			, RefIncidentLocation.Description 'refIncidentLocation'                                --13
			, RefFirearmType.Description 'refFirearmType'                                          --14
			, json_valid(I.RegulationViolatedDescription) as regulationViolatedDescriptionBoolean  --15		 
			, I.regulationViolatedDescription			                                                 --16
			, I.relatedToDisabilityManifestationInd                                                --17
			, I.reportedToLawEnforcementInd                                                        --18
			, RefIncidentMultipleOffenseType.Description 'refIncidentMultipleOffenseType'          --19
			, RefIncidentPerpetratorInjuryType.Description 'refIncidentPerpetratorInjuryType'      --20
			, I.organizationId			                                                               --21
			
			, incP.personId                                                                        --22
			, RefIncidentPersonRoleType.Description 'refIncidentPersonRoleType'                    --23
			, incP.identifier                                                                      --24
			, RefIncidentPersonType.Description 'refIncidentPersonType'                            --25
			, incP.date                                                                            --26
			, incP.fileScanBase64                                                                  --27
			, incP.digitalRandomKey                                                                --28
			
			, k12disc.k12StudentDisciplineId                                                       --29
			, k12disc.organizationPersonRoleId                                                     --30
			, k12disc.refDisciplineReasonId                                                        --31
			, k12disc.refDisciplinaryActionTakenId                                                 --32
			, k12disc.disciplinaryActionStartDate                                                  --33
			, k12disc.disciplinaryActionEndDate                                                    --34
			, k12disc.durationOfDisciplinaryAction                                                 --35
			, k12disc.refDisciplineLengthDifferenceReasonId                                        --36
			, k12disc.fullYearExpulsion                                                            --37
			, k12disc.shortenedExpulsion                                                           --38
			, k12disc.educationalServicesAfterRemoval                                              --39
			, k12disc.refIdeaInterimRemovalId                                                      --40
			, k12disc.refIdeaInterimRemovalReasonId                                                --41
			, k12disc.relatedToZeroTolerancePolicy                                                 --42
			, k12disc.iEPPlacementMeetingIndicator                                                 --43
			, k12disc.refDisciplineMethodFirearmsId                                                --44
			, k12disc.refDisciplineMethodOfCwdId                                                   --45
			, k12disc.refIDEADisciplineMethodFirearmId                                             --46
			, k12disc.personId                                                                     --47
			
            FROM Incident I

			OUTER LEFT JOIN IncidentPerson incP
				ON I.IncidentId = incP.IncidentId

			OUTER LEFT JOIN K12StudentDiscipline k12disc
				ON I.IncidentId = k12disc.IncidentId
				
			OUTER LEFT JOIN RefIncidentTimeDescriptionCode USING(RefIncidentTimeDescriptionCodeId)
			OUTER LEFT JOIN RefIncidentBehavior USING(RefIncidentBehaviorId)
			OUTER LEFT JOIN RefIncidentInjuryType USING(RefIncidentInjuryTypeId)
			OUTER LEFT JOIN RefWeaponType USING(RefWeaponTypeId)
			
			OUTER LEFT JOIN RefIncidentReporterType USING(RefIncidentReporterTypeId)
			OUTER LEFT JOIN RefIncidentLocation USING(RefIncidentLocationId)
			OUTER LEFT JOIN RefFirearmType USING(RefFirearmTypeId)
			
			OUTER LEFT JOIN RefIncidentMultipleOffenseType USING(RefIncidentMultipleOffenseTypeId)
			OUTER LEFT JOIN RefIncidentPerpetratorInjuryType USING(RefIncidentPerpetratorInjuryTypeId)
			
			OUTER LEFT JOIN RefIncidentPersonRoleType USING(RefIncidentPersonRoleTypeId)
			OUTER LEFT JOIN RefIncidentPersonType USING(RefIncidentPersonTypeId)
	
            WHERE  
				I.RecordEndDateTime IS NULL
				AND 
				incP.RecordEndDateTime IS NULL
				AND 
				k12disc.RecordEndDateTime IS NULL
			ORDER BY I.incidentId
    """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_queryIncident} -> {str(e)}")

    if(len(_queryIncident) <= 0):
        logger.info(f"S/Datos ")
        logger.info(f"Sin incidentes registrados")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r

    _e = []
    try:
        entrevistado = 0
        entrevistador = 0
        asistente = 0
        dirige = 0
        for i, _incident in enumerate(_queryIncident):
            def _err(msg): return {
                "incidentId": _incident[0], "errorDescription": msg}
            _incidentePrevio = None if (i > 0) else _queryIncident[i-1][0]

            if not _incident[1]:
                _e.append(_err(f"Campo incidentDate is NULL"))
            if not _incident[2]:
                _e.append(_err(f"Campo incidentTime is NULL"))
            if not _incident[3]:
                _e.append(_err(f"Campo refIncidentTimeDescriptionCode is NULL"))
            if not _incident[4]:
                _e.append(_err(f"Campo incidentDescription is NULL"))
            if not _incident[5]:
                _e.append(_err(f"Campo refIncidentBehavior is NULL"))
            if _incident[5] not in ('Entrevista', 'Reunión con apoderados'):
                if not _incident[11]:
                    _e.append(_err(f"Campo incidentReporterId is NULL"))
                if not _incident[12]:
                    _e.append(_err(f"Campo refIncidentReporterType is NULL"))

            if not _incident[22]:
                _e.append(_err(f"Campo personId is NULL"))
            if not _incident[23]:
                _e.append(_err(f"Campo refIncidentPersonRoleType is NULL"))
            if not _incident[25]:
                _e.append(_err(f"Campo refIncidentPersonType is NULL"))
            if not _incident[26]:
                _e.append(_err(f"Campo date is NULL"))
            if(_incident[25] in ('Docente', 'Profesional de la educación', 'Personal Administrativo')):
                if not _incident[27] and not _incident[28]:
                    _e.append(
                        _err(f"Campo digitalRandomKey y fileScanBase64 are NULL"))

            if(_incident[5] not in ('Entrevista', 'Reunión con apoderados', 'Entrega de documentos retiro de un estudiante', 'Anotación positiva', 'Entrega de documentos de interés general', 'Entrega de información para continuidad de estudios')):  # Anotaciones negativas
                if not check_utils.validateJSON(_incident[16]):
                    _e.append(
                        _err(f"Campo regulationViolatedDescription is NOT JSON"))

            if(_incident[5] in ('Entrega de documentos retiro de un estudiante', 'Entrega de documentos de interés general', 'Entrega de información para continuidad de estudios')):
                pass

            if(_incident[5] == 'Entrevista'):
                if(_incident[23] == 'Entrevistado'):
                    if (_incident[25] not in ('Apoderado', 'Adulto responsable o conocido del estudiante', 'Parent/guardian')):
                        _e.append(_err(
                            f"Campo Tipo 'Adulto responsable o conocido del estudiante' está mal aplicado. (1){_incident[5]}...{_incident[23]}...{_incident[25]}"))
                        entrevistado += 1
                if(_incident[23] == 'Entrevistador'):
                    if(_incident[25] not in ('Docente', 'Profesional de la educación', 'Substitute teacher', 'Personal Administrativo')):
                        _e.append(_err(
                            f"Campo Tipo 'Dirige reunión de apoderados' está mal aplicado. (2){_incident[5]}...{_incident[23]}...{_incident[25]}"))
                        entrevistador += 1
                if(_incidentePrevio is not None and _incidentePrevio != _incident[0]):
                    if(entrevistado == 0):
                        _e.append(
                            _err(f"Falto definir el entrevistado en el incidente"))
                        entrevistado = 0
                    if(entrevistador == 0):
                        _e.append(
                            _err(f"Falto definir el entrevistador en el incidente"))
                        entrevistador = 0

            if(_incident[5] == 'Reunión con apoderados'):
                if(_incident[23] == 'Asiste a reunión de apoderados'):
                    if(_incident[25] not in ('Apoderado', 'Adulto responsable o conocido del estudiante', 'Parent/guardian')):
                        _e.append(_err(
                            f"Campo Tipo 'Adulto responsable o conocido del estudiante' está mal aplicado. (3){_incident[5]}...{_incident[23]}...{_incident[25]}"))
                        asistente += 1
                if(_incident[23] == 'Dirige reunión de apoderados'):
                    if (_incident[25] not in ('Docente', 'Profesional de la educación', 'Substitute teacher')):
                        _e.append(_err(
                            f"Campo Tipo 'Dirige reunión de apoderados' está mal aplicado. (4){_incident[5]}...{_incident[23]}...{_incident[25]}"))
                        dirige += 1
                if(_incidentePrevio is not None and _incidentePrevio != _incident[0]):
                    if(asistente == 0):
                        _e.append(
                            _err(f"Falto definir el asistente en el incidente"))
                        asistente = 0
                    if(dirige == 0):
                        _e.append(
                            _err(f"Falto definir el dirige en el incidente"))
                        dirige = 0

            if(_incident[5] == 'Anotación positiva'):
                if not check_utils.validateJSON(_incident[16]):
                    _e.append(
                        _err(f"Campo regulationViolatedDescription is NOT JSON"))

        if(len(_e) == 0):
            _r = True
        else:
            logger.error(_e)
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        logger.info(f'Aprobado') if _r else logger.error(f'Rechazado')
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
