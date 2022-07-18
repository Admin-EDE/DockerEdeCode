from inspect import getframeinfo, currentframe
from multiprocessing import current_process
from datetime import datetime

from ede.ede._logger import logger

def fn6F1(conn, return_dict):
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
    arr=[]
    try:
           
      _S1= """ 
select 
	strftime('%d',rae.Date) as Dia,
	strftime('%m',rae.Date) as Mes,
	strftime('%Y',rae.Date) as Año,
	pid.Identifier as Numerolista ,
	rae.VirtualIndicator 
FROM PersonIdentifier pid 
	JOIN  OrganizationPersonRole opr 
		on opr.personId = pid.personId
	JOIN RoleAttendanceEvent rae 
		on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
where 
	pid.RefPersonIdentificationSystemId IN (
		SELECT RefPersonIdentificationSystemId
		FROM RefPersonIdentificationSystem
		WHERE RefPersonIdentificationSystem.Description IN ('Número de lista')
	)
	AND
	opr.roleid IN (
		SELECT RoleId
		FROM Role
		WHERE Name IN ('Estudiante')
	)
	AND
	rae.VirtualIndicator = 0
         """
    

      now=datetime.now()
      _q1 = conn.execute(_S1).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          dia=str(q1[0])
          mes=str(q1[1])
          año=str(q1[2])
          numero_l=str(q1[3])
          estado_asis=str(q1[4])
          
          if (dia is None) or (mes is None) or (año is None) or (numero_l is None) or (estado_asis is None): 
            arr.append(numero_l)

          if int(estado_asis)==0:
            asistencia="Presencial"

          if(len(arr)!=0):
              logger.error(f"Los siguientes numero de lista necesita informacion: {str(arr)} ")
              logger.error(f"Rechazado")
              return_dict[getframeinfo(currentframe()).function] = False
              return False
          else:
              logger.info(f"Ningunos de los registros le falta un dato.")
              logger.info(f"Aprobado")
              return_dict[getframeinfo(currentframe()).function] = True
              return True
      else:
        logger.error(f"No hay registro Numero de lista asociados .")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = False
      return False
### fin fn6f1  ###