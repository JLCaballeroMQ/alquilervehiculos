--creacion de PROCEDIMIENTOS ALMACENADOS
--JLCaballeroMQ



create or replace procedure alquilar(arg_NIF_cliente varchar,
  arg_matricula varchar, arg_fecha_ini date, arg_fecha_fin date) is
   e_invalid_cliente EXCEPTION;
   e_invalid_vehiculo EXCEPTION;
   e_invalid_reserva EXCEPTION;
  Importe_mal EXCEPTION;
   importe_factura integer;
   numero_Dias number := 0;
  v_countReservas number := 0;
 v_countClientes number := 0;
varNumvehiculos number := 0;
precioDia integer;
begin
--Punto(1) Se Comprueba que la fecha de inicio pasada como argumento no es posterior a la fecha fin En caso contrario devolverá el error -20003 con el mensaje 'El numero de dias sera mayor que cero.'
if(arg_fecha_ini< arg_fecha_fin) then

 dbms_output.put_line( 'inicio');
-- Punto 2 SELECT con un par de joins para saber el valor del modelo del vehículo pasado como argumento, el prcio de alquilarlo diariamente, la capacidad de su depósito de combustible, el tipo de combustible que utiliza y el precio por litro del mismo
select modelos.precio_cada_dia into precioDia 
from vehiculos
inner join modelos
on 
modelos.id_modelo = vehiculos.id_modelo
inner join precio_combustible
on 
modelos.tipo_combustible = precio_combustible.tipo_combustible
where vehiculos.matricula = arg_matricula;

  dbms_output.put_line('precio');
 dbms_output.put_line(precioDia);
  
select count(*) INTO  varNumvehiculos
from vehiculos
inner join modelos
on 
modelos.id_modelo = vehiculos.id_modelo
inner join precio_combustible
on 
modelos.tipo_combustible = precio_combustible.tipo_combustible
 where vehiculos.matricula = arg_matricula;



--Del resultado de esta SELECT deberías ser capaz de deducir si el vehículo existe. Si no existiese has de devolver el error -20002 con el mensaje 'Vehiculo inexistente.'.

if varNumvehiculos = 0 then

rollback;
RAISE e_invalid_vehiculo;
end if;  
 --punto 3
 select count(*) into v_countReservas from reservas where arg_matricula=reservas.matricula AND fecha_ini=arg_fecha_ini and fecha_fin=arg_fecha_fin;
 
if v_countReservas != 0
then
Raise e_invalid_reserva;
else
 dbms_output.put_line( 'reserva disponible');
--punto 4
--el resultado de la SELECT del paso anterior ¿sigue siendo fiable en este paso?:
--en este paso no es fiable la informacion del cliente pues no se encuentra bloqueada dicha tabla por lo cual puede estar ocurriendo una actualizacion en otro punto
--1. En este paso, la ejecución concurrente del mismo procedimiento ALQUILA con, quizás otros o los mimos argumentos, ¿podría habernos añadido una reserva no recogida en esa SELECT que fuese incompatible con nuestra reserva?, ¿por qué?
-- si no se bloquea las tablas mientras ocurren los procesos de insercion y actualizacion que estan concatenadas con otras tablas, se puede dar q en otro punto y en el mismo instante de tiempo se haga una reserva q cumpla con los parametros q se estan revisando y no se tenga en cuenta dicha infformacion en este select
--En este paso otra transacción concurrente cualquiera ¿podría hacer INSERT o UPDATE sobre reservas y habernos añadido una reserva no recogida en esa SELECTque fuese incompatible con nuestra reserva?, ¿por qué?.
--si la transaccion no se cierra desde el comienzo se puede estar dividiendo en varias, y se estaria rompiendo la consistencia de los datos, pero en este punto dado que en el punto anterior se realizo el bloqueo del select del vehiculo no se tendrian reservas incompatibles con nuestro proceso.
select count(*) into v_countClientes from clientes where arg_NIF_cliente=clientes.NIF;
if v_countClientes = 0
then
RAISE  e_invalid_cliente;

else
 dbms_output.put_line( 'cliente disponible');
--Insertamos una fila en la tabla de reservas para el cliente, vehículo e intervalo de fechas pasado como argumento. En esta operación deberíamos ser capaces de detectar si el cliente no existe, en cuyo caso lanzaremos la excepción -20001, con el mensaje 'Cliente inexistente'.
INSERT INTO reservas  
(idReserva,cliente, matricula, fecha_ini,fecha_fin)  
VALUES  
(seq_reservas.NEXTVAL,arg_NIF_cliente, arg_matricula, arg_fecha_ini, arg_fecha_fin );   
 dbms_output.put_line( 'insercion realizada');
 --punto 5  Se crea una factura correspondiente a alquilar al cliente con ese NIF el vehículo con esa matrícula durante los días transcurridos: n_dias = fecha_fin – fecha_ini.
--El campo importe de la factura se rellena con la suma de los importes de las líneas de
--factura, que se crearán como se indica más adelante.

 INSERT INTO facturas  
(nroFactura,importe, cliente)  
VALUES  
(seq_num_fact.NEXTVAL,(SELECT
    SUM(importe)
FROM
    lineas_factura where lineas_factura.nroFactura=nroFactura)  ,arg_NIF_cliente );   
end if;
 dbms_output.put_line( 'factura creada');
  numero_Dias:= arg_fecha_fin- arg_fecha_ini;
   dbms_output.put_line(  numero_Dias);
INSERT INTO lineas_factura  
(nroFactura,concepto, importe)  
VALUES  
(seq_num_fact.NEXTVAL-1,'alquiler vehículo',numero_Dias*precioDia  );   

   dbms_output.put_line(  'linea de factura creada');
end if;
else
raise_application_error(-20003, 'El numero de dias sera mayor que cero');
end if;
  exception  
   WHEN e_invalid_cliente THEN
   
    dbms_output.put_line('cliente inexistente');
   WHEN e_invalid_vehiculo THEN
   
    dbms_output.put_line('vehiculo inexistente');

WHEN e_invalid_reserva THEN
   
    dbms_output.put_line('reserva existente'); 
    when others then
       if sqlcode=-20003 then
        dbms_output.put_line('El numero de dias sera mayor que cero');
     
      end if;

end;
/






create or replace
procedure reset_seq( p_seq_name varchar )
--From https://stackoverflow.com/questions/51470/how-do-i-reset-a-sequence-in-oracle
is
    l_val number;
begin
    --Averiguo cual es el siguiente valor y lo guardo en l_val
    execute immediate
    'select ' || p_seq_name || '.nextval from dual' INTO l_val;

    --Utilizo ese valor en negativo para poner la secuencia cero, pimero cambiando el incremento de la secuencia
    execute immediate
    'alter sequence ' || p_seq_name || ' increment by -' || l_val || 
                                                          ' minvalue 0';
   --segundo pidiendo el siguiente valor
    execute immediate
    'select ' || p_seq_name || '.nextval from dual' INTO l_val;

    --restauro el incremento de la secuencia a 1
    execute immediate
    'alter sequence ' || p_seq_name || ' increment by 1 minvalue 0';

end;
/
create or replace procedure inicializa_test is
begin
  reset_seq( 'seq_modelos' );
  reset_seq( 'seq_num_fact' );
  reset_seq( 'seq_reservas' );
        
  
    delete from lineas_factura;
    delete from facturas;
    delete from reservas;
    delete from vehiculos;
    delete from modelos;
    delete from precio_combustible;
    delete from clientes;
   
		
    insert into clientes values ('12345678A', 'Pepe', 'Perez', 'Porras', 'C/Perezoso n1');
    insert into clientes values ('11111111B', 'Beatriz', 'Barbosa', 'Bernardez', 'C/Barriocanal n1');
    
    insert into precio_combustible values ('Gasolina', 1.5);
    insert into precio_combustible values ('Gasoil',   1.4);
    
    insert into modelos values ( seq_modelos.nextval, 'Renault Clio Gasolina', 15, 50, 'Gasolina');
    insert into vehiculos values ( '1234-ABC', seq_modelos.currval, 'VERDE');

    insert into modelos values ( seq_modelos.nextval, 'Renault Clio Gasoil', 16,   50, 'Gasoil');
    insert into vehiculos values ( '1111-ABC', seq_modelos.currval, 'VERDE');
    insert into vehiculos values ( '2222-ABC', seq_modelos.currval, 'GRIS');
	
    commit;
end;
/
exec inicializa_test;


create or replace procedure test_alquila_coches is
begin
	 
  --caso 1 nro dias negativo
  begin
    inicializa_test;
    alquilar('12345678A', '1234-ABC', current_date, current_date-1);
    dbms_output.put_line('MAL: Caso nro dias negativo no levanta excepcion');
  exception
    when others then
      if sqlcode=-20003 then
        dbms_output.put_line('OK: Caso nro dias negativo correcto');
      else
        dbms_output.put_line('MAL: Caso nro dias negativo levanta excepcion '||sqlcode||' '||sqlerrm);
      end if;
  end;
  
  --caso 2 vehiculo inexistente
  begin
    inicializa_test;
    alquilar('87654321Z', '9999-ZZZ', date '2013-3-20', date '2013-3-22');
    dbms_output.put_line('MAL: Caso vehiculo inexistente no levanta excepcion');
  exception
    when others then
      if sqlcode=-20002 then
        dbms_output.put_line('OK: Caso vehiculo inexistente correcto');
      else
        dbms_output.put_line('MAL: Caso vehiculo inexistente levanta excepcion '||sqlcode||' '||sqlerrm);
      end if;
  end;
  
  --caso 3 cliente inexistente
  begin
    inicializa_test;
    alquilar('87654321Z', '1234-ABC', date '2013-3-20', date '2013-3-22');
    dbms_output.put_line('MAL: Caso cliente inexistente no levanta excepcion');
  exception
    when others then
      if sqlcode=-20001 then
        dbms_output.put_line('OK: Caso cliente inexistente correcto');
      else
        dbms_output.put_line('MAL: Caso cliente inexistente levanta excepcion '||sqlcode||' '||sqlerrm);
      end if;
  end;
  
  --caso 4 Todo correcto pero NO especifico la fecha final 
  declare
                
    resultadoPrevisto varchar(200) := 
      '1234-ABC11/03/1313512345678A4 dias de alquiler, vehiculo modelo 1   60#'||
      '1234-ABC11/03/1313512345678ADeposito lleno de 50 litros de Gasolina 75';
                
    resultadoReal varchar(200)  := '';
    fila varchar(200);
  begin  
    inicializa_test;
    alquilar('12345678A', '1234-ABC', date '2013-3-11', null);
    
    SELECT listAgg(matricula||fecha_ini||fecha_fin||facturas.importe||cliente
								||concepto||lineas_factura.importe, '#')
            within group (order by nroFactura, concepto)
    into resultadoReal
    FROM facturas join lineas_factura using(NroFactura)
                  join reservas using(cliente);
								
    dbms_output.put_line('Caso Todo correcto pero NO especifico la fecha final:');
   if resultadoReal=resultadoPrevisto then
      dbms_output.put_line('--OK SI Coinciden la reserva, la factura y las linea de factura');
    else
      dbms_output.put_line('--MAL NO Coinciden la reserva, la factura o las linea de factura');
      dbms_output.put_line('resultadoPrevisto='||resultadoPrevisto);
      dbms_output.put_line('resultadoReal    ='||resultadoReal);
    end if;
    
  exception   
    when others then
       dbms_output.put_line('--MAL: Caso Todo correcto pero NO especifico la fecha final devuelve '||sqlerrm);
  end;
  
  --caso 5 Intentar alquilar un coche ya alquilado
  
  --5.1 la fecha ini del alquiler esta dentro de una reserva
  begin
    inicializa_test;    
	--Reservo del 2013-3-10 al 12
	insert into reservas values
	 (seq_reservas.NEXTVAL, '11111111B', '1234-ABC', date '2013-3-11'-1, date '2013-3-11'+1);
    --Fecha ini de la reserva el 11 
	alquilar('12345678A', '1234-ABC', date '2013-3-11', date '2013-3-13');
	
    dbms_output.put_line('MAL: Caso vehiculo ocupado solape de fecha_ini no levanta excepcion');
	
  exception
    when others then
      if sqlcode=-20004 then
        dbms_output.put_line('OK: Caso vehiculo ocupado solape de fecha_ini correcto');
      else
        dbms_output.put_line('MAL: Caso vehiculo ocupado solape de fecha_ini levanta excepcion '||sqlcode||' '||sqlerrm);
      end if;
  end; 
  
   --5.2 la fecha fin del alquiler esta dentro de una reserva
  begin
    inicializa_test;    
	--Reservo del 2013-3-10 al 12
	insert into reservas values
	 (seq_reservas.NEXTVAL, '11111111B', '1234-ABC', date '2013-3-11'-1, date '2013-3-11'+1);
    --Fecha fin de la reserva el 11 
	alquilar('12345678A', '1234-ABC', date '2013-3-7', date '2013-3-11');
	
    dbms_output.put_line('MAL: Caso vehiculo ocupado solape de fecha_fin no levanta excepcion');
	
  exception
    when others then
      if sqlcode=-20004 then
        dbms_output.put_line('OK: Caso vehiculo ocupado solape de fecha_fin correcto');
      else
        dbms_output.put_line('MAL: Caso vehiculo ocupado solape de fecha_fin levanta excepcion '||sqlcode||' '||sqlerrm);
      end if;
  end; 
  
  --5.3 la el intervalo del alquiler esta dentro de una reserva
  begin
    inicializa_test;    
	--Reservo del 2013-3-9 al 13
	insert into reservas values
	 (seq_reservas.NEXTVAL, '11111111B', '1234-ABC', date '2013-3-11'-2, date '2013-3-11'+2);
    -- reserva del 4 al 19
	alquilar('12345678A', '1234-ABC', date '2013-3-11'-7, date '2013-3-12'+7);
	
    dbms_output.put_line('MAL: Caso vehiculo ocupado intervalo del alquiler esta dentro de una reserva no levanta excepcion');
	
  exception
    when others then
      if sqlcode=-20004 then
        dbms_output.put_line('OK: Caso vehiculo ocupado intervalo del alquiler esta dentro de una reserva correcto');
      else
        dbms_output.put_line('MAL: Caso vehiculo ocupado intervalo del alquiler esta dentro de una reserva levanta excepcion '
        ||sqlcode||' '||sqlerrm);
      end if;
  end; 
  
   --caso 6 Todo correcto pero SI especifico la fecha final 
  declare
                                      
    resultadoPrevisto varchar(400) := '12222-ABC11/03/1313/03/1310212345678A2 dias de alquiler, vehiculo modelo 2   32#'||
                                    '12222-ABC11/03/1313/03/1310212345678ADeposito lleno de 50 litros de Gasoil   70';
                                      
    resultadoReal varchar(400)  := '';    
    fila varchar(200);
  begin
    inicializa_test;
  
    SELECT listAgg(nroFactura||matricula||fecha_ini||fecha_fin||facturas.importe||cliente
								||concepto||lineas_factura.importe, '#')
            within group (order by nroFactura, concepto)
    into resultadoReal
    FROM facturas join lineas_factura using(NroFactura)
                  join reservas using(cliente);
    
    
    dbms_output.put_line('Caso Todo correcto pero SI especifico la fecha final');
    
    if resultadoReal=resultadoPrevisto then
      dbms_output.put_line('--OK SI Coinciden la reserva, la factura y las linea de factura');
    else
      dbms_output.put_line('--MAL NO Coinciden la reserva, la factura o las linea de factura');
      dbms_output.put_line('resultadoPrevisto='||resultadoPrevisto);
      dbms_output.put_line('resultadoReal    ='||resultadoReal);
    end if;
    
  exception   
    when others then
       dbms_output.put_line('--MAL: Caso Todo correcto pero SI especifico la fecha final devuelve '||sqlerrm);
  end;
 
end;
/

set serveroutput on
exec test_alquila_coches;




