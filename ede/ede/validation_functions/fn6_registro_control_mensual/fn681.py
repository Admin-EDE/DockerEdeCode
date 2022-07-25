from inspect import getframeinfo, currentframe
from multiprocessing import current_process
from datetime import datetime

from ede.ede._logger import logger


def fn681(conn, return_dict):
    """ Breve descripción de la función
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
    arr = []
    arr2 = []
    arr3 = []
    dias_laborales = []
    dias_laborales2 = []
    numero = 0
    try:

        _S1 = """ SELECT OrganizationId
                FROM Organization
                WHERE RefOrganizationTypeId = 47;"""

        _S2 = """ SELECT Parent_OrganizationId
                FROM OrganizationRelationship
                WHERE OrganizationId = ?;"""

        _S3 = """ SELECT OrganizationId
                FROM K12Course
                WHERE OrganizationId = ? and RefWorkbasedLearningOpportunityTypeId=1 ;"""

        _S4 = """ select personid 
              from OrganizationPersonRole
              where OrganizationId=? and RoleId = 6 ;"""

        _S5 = """ select b.Identifier
              from PersonStatus a 
              join personidentifier b 
              on  a.personid = b.personId  
              where a.RefPersonStatusTypeId=35 and a.personid=? """

        now = datetime.now()
        _q1 = conn.execute(_S1).fetchall()
        XX = 0
        if(len(_q1) != 0):
            for q1 in _q1:
                parent = str(q1[0])
                _q2 = conn.execute(_S2, parent).fetchall()
                if(len(_q2) != 0):
                    for q2 in _q2:
                        parent2 = str(q2[0])
                        _q3 = conn.execute(_S3, parent2).fetchall()
                        if(len(_q3) != 0):
                            for q3 in _q3:
                                parent3 = str(q3[0])
                                _q4 = conn.execute(_S4, parent3).fetchall()
                                if(len(_q4) != 0):
                                    for q4 in _q4:
                                        personid = str(q4[0])
                                        _q5 = conn.execute(
                                            _S5, personid).fetchall()
                                        if(len(_q5) == 0):
                                            rut = str(_q5[0])
                                            arr.append(rut)

                                    if(len(arr) != 0):
                                        logger.error(
                                            f"Los siguientes alumnos no tienen identificador de Formacion Dual : {str(arr)} ")
                                        logger.error(f"Rechazado")
                                        return_dict[getframeinfo(
                                            currentframe()).function] = False
                                        return False
                                    else:
                                        logger.info(f"Aprobado")
                                        return_dict[getframeinfo(
                                            currentframe()).function] = True
                                        return True

                                else:
                                    logger.error(
                                        f"No tiene alumnos en la asignatura ")
                                    logger.error(f"Rechazado")
                                    return_dict[getframeinfo(
                                        currentframe()).function] = False
                                    return False

                        else:
                            logger.error(
                                f"La asignatura no esta enlazada para que sea de partica profesional")
                            logger.error(f"Rechazado")
                            return_dict[getframeinfo(
                                currentframe()).function] = False
                            return False

        else:
            logger.info(
                f"En el colegio no hay asignaturas de pratica profesional.")
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            return True

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
