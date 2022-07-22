from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn8F3(conn, return_dict):
    """ 
    REGISTRO DE ANOTACIONES DE CONVIVENCIA ESCOLAR POR ESTUDIANTE
      6.2 Contenido mínimo, letra e
      Verificar que las entrevistas con el apoderado y su contenido se 
      encuentre cargado en el sistema.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información.
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - La Tabla Incident debería almacenar las entrevistas con los apoderados. 
            Si éstas requieren firma, deberiamos incluir el campo para el scaneo y el 
            verificador de identidad, según corresponda.
            - Verificar si es necesario incluir un código especial para las entrevistas, 
            de modo que sea más sencillo filtrarlas.
            - Incident.RefIncidentBehaviorId == 31 (Entrevista) OR 
            Incident.RefIncidentBehaviorId == 32 (Reunión con apoderados)
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    allRows = []
    try:
        allRows = conn.execute("""
          SELECT inc.IncidentId
          FROM Incident inc
          JOIN RefIncidentBehavior rib
            ON rib.RefIncidentBehaviorId = inc.RefIncidentBehaviorId
            AND rib.Description IN ('Entrevista','Reunión con apoderados') 
    """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {allRows} -> {str(e)}")

    if(len(allRows) == 0):
        logger.info(f'S/Datos')
        logger.info(
            f'No hay entrevistas o reuniones con apoderados registradas en el sistema')
        return_dict[getframeinfo(currentframe()).function] = True
        return True
    FineRows = []
    try:
        FineRows = conn.execute("""
          SELECT inc.IncidentId
          FROM Incident inc
          JOIN RefIncidentBehavior rib
            ON rib.RefIncidentBehaviorId = inc.RefIncidentBehaviorId
            AND rib.Description IN ('Entrevista','Reunión con apoderados')
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
        if(len(allRows) > 0):
            resultList = [item[0] for item in allRows if item not in FineRows]

        if(len(resultList) > 0):
            logger.error(f"Rechazado")
            logger.info(f"Los incidentId con problemas son: {resultList}")
        else:
            logger.info(f"Aprobado")
            _r = True

    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r
