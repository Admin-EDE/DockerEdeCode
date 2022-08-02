from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn1FB(conn, return_dict):
    """
    REGISTRO DE LA ENTREGA DE INFORMACIÓN
    8.0 De la entrega de información
    verifica que exista cargado en la base de datos el documento digital o verificador de identidad que acredite la entrega al 
    apoderado información de interés general, tal como:
    - Reglamento interno
    - Reglamento de evaluación y promoción
    - Proyecto Educativo
    - Programa de seguridad escolar
    - entre otros, salvo aquellos de carácter confidencial o de uso personal.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger, si no existe información en el sistema.
          Retorna True y "Aprobado" a través de logger, si cada estudiante cumple con los siguientes criterios:
            - Revisar que la entrega de documentos se encuentre cargada en las incidencias como un tipo de reunión con el apoderado.
            - En tabla Indicent.RefIncidentBehaviorId == 35 (Entrega de documentos de interés general) y }
            IncidentPerson.digitalRandomKey OR fileScanBase64 según sea el caso
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    Allrows = []
    try:
        Allrows = conn.execute("""--sql
        SELECT inc.IncidentId
        FROM Incident inc
        JOIN RefIncidentBehavior rib
          ON rib.RefIncidentBehaviorId = inc.RefIncidentBehaviorId
          AND rib.Description IN ('Entrega de documentos de interés general')
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {Allrows} -> {str(e)}")

    if(len(Allrows) == 0):
        logger.info("S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True

    _r = False
    FineRows = []
    try:
        FineRows = conn.execute("""--sql
          SELECT inc.IncidentId
          FROM Incident inc
          JOIN RefIncidentBehavior rib
            ON rib.RefIncidentBehaviorId = inc.RefIncidentBehaviorId
            AND rib.Description IN ('Entrega de documentos de interés general')
          JOIN IncidentPerson iper
            ON iper.IncidentId = inc.IncidentId
            AND iper.fileScanBase64 IS NOT NULL
          JOIN Document doc
            ON doc.documentId = iper.fileScanBase64
          JOIN RefIncidentPersonType ript
            ON ript.RefIncidentPersonTypeId = iper.RefIncidentPersonTypeId
            AND ript.Description IN ('Apoderado')
          JOIN PersonRelationship prsh
            ON prsh.personId = iper.personId
          JOIN RefPersonRelationship rprsh
            ON rprsh.RefPersonRelationshipId = prsh.RefPersonRelationshipId
            AND rprsh.Code IN ('Apoderado(a)/Tutor(a)')
          JOIN OrganizationPersonRole opr
            ON opr.personId = iper.personId
          JOIN Role rol
            ON rol.RoleId = opr.RoleId
            AND rol.Name IN ('Padre, madre o apoderado')
          GROUP BY inc.IncidentId
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {FineRows} -> {str(e)}")

    resultList = []
    try:
        if(len(Allrows) > 0):
            resultList = [item[0] for item in Allrows if item not in FineRows]

        if(len(resultList) > 0):
            logger.error(f"Rechazado")
            _r = False
            logger.info(f"Los incidentId con problemas son: {resultList}")
        else:
            logger.info(f"Aprobado")
            _r = True

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
