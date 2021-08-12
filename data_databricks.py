import json
import boto3
import pandas as pd
from io import StringIO


s3= boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_name = 'nombre_bucket'
key_name = 'direccion_bucket1'
key_name2 = 'direccion_bucket2'
key_name3 = 'direccion_bucket3'
def lambda_handler(event, context):
    #objs=list(bucket.objects.filter(Prefix=key_name))
    resp = s3.get_object(Bucket=bucket_name, Key=key_name)
    if (len(resp)>0):
        trama = pd.read_csv(resp['Body'], sep='\t', header=None)
        df = pd.DataFrame(trama)
        total=df.shape[0]
        total
        df[0] = df[0].str.replace('#','Ñ')
        df[0] = df[0].str.replace(',',' ')
        datos ={}
        for i in range(total):
            c1=df[0][i]
            c100=c1[:]
            c101=c1[:3]
            c102=c1[639:641]
            datos[i]=[c100.rstrip(),c101.rstrip(),c102.rstrip()]
        df3=pd.DataFrame(datos).T
        csv_buffer = StringIO()
        df3.columns = ["datos","Producto","plan"]
        groups2=df3.groupby(df3["Producto"])
        lista2 = df3["Producto"].unique().tolist()
        for x in lista2:

            if x == "803":
                csv_buffer.truncate(0)
                csv_buffer.seek(0)
                M805=groups2.get_group(x)
                M807=M805[M805["plan"]!="01"]
                M806=M807.drop(["Producto","plan"], axis=1)
                count=M806.shape[0]
                #M806.to_csv(f'D:\dataexport\M{x}_TLM.txt', header = None, sep=',',index = None)
                M806.to_csv(csv_buffer, header = None, sep=',',index = None)
                s3_resource.Object(bucket_name, key_name2 + f'M{x}_TLM.txt').put(Body=csv_buffer.getvalue())
                
                csv_buffer.truncate(0)
                csv_buffer.seek(0)
                M805=M805[M805["plan"]=="01"]
                M806=M805.drop(["Producto","plan"], axis=1)
                #M806.to_csv(f'D:\dataexport\M{x}_01.txt', header = None, sep=',',index = None)
                M806.to_csv(csv_buffer, header = None, sep=',',index = None)
                s3_resource.Object(bucket_name, key_name2 + f'M{x}_01.txt').put(Body=csv_buffer.getvalue())
            else:
                csv_buffer.truncate(0)
                csv_buffer.seek(0)
                M805=groups2.get_group(x)
                M806=M805.drop(["Producto","plan"], axis=1)
                #M806.to_csv(f'D:\dataexport\M{x}.txt', header = None, sep=',',index = None)
                M806.to_csv(csv_buffer, header = None, sep=',',index = None)
                s3_resource.Object(bucket_name, key_name2 + f'M{x}.txt').put(Body=csv_buffer.getvalue())


        # csv_buffer = StringIO()
        # df.to_csv(csv_buffer)
        # s3_resource.Object(bucket_name, key_name2).put(Body=csv_buffer.getvalue())
               
    else:
        print("No existe el recurso")

    datos2 ={}
    for i in range(total):
        c1=df[0][i]
        c101=c1[:3]
        c102=c1[3:23]
        c103=c1[23:27]
        c104=c1[27:29]
        c105=c1[29:43]
        c106=c1[43:45]
        c107=c1[45:48]
        c108=c1[48:49]
        c109=c1[49:79]
        c110=c1[79:109]
        c111=c1[109:139]
        c112=c1[139:140]
        c113=c1[140:141]###
        c114=c1[141:149]
        c115=c1[149:150]
        c116=c1[150:160]
        c117=c1[160:346]
        c118=c1[346:347]
        c119=c1[347:368]
        c120=c1[368:376]
        c121=c1[376:384]
        c122=c1[384:392]
        c123=c1[392:396]
        c124=c1[396:410]
        c125=c1[410:411]
        c126=c1[411:425]
        c127=c1[425:426]
        c128=c1[426:439]
        c129=c1[439:441]
        c130=c1[441:635]
        c131=c1[635:639]
        c132=c1[639:641]
        c133=c1[641:]
        datos[i]=[c101.rstrip(),c102.rstrip(),c103.rstrip(),c104.rstrip(),c105.rstrip(),c106.rstrip(),c107.rstrip(),
        c108.rstrip(),c109.rstrip(),c110.rstrip(),c111.rstrip(),c112.rstrip(),c113.rstrip(),c114.rstrip(),c115.rstrip(),
        c116.rstrip(),c117.rstrip(),c118.rstrip(),c119.rstrip(),c120.rstrip(),c121.rstrip(),c122.rstrip(),c123.rstrip(),
        c124.rstrip(),c125.rstrip(),c126.rstrip(),c127.rstrip(),c128.rstrip(),c129.rstrip(),c130.rstrip(),c131.rstrip(),
        c132.rstrip(),c133.rstrip()]
    df2=pd.DataFrame(datos).T
    df2.columns = ["Codigo de Seguro","Nro Certificado","CodigoOficina","Codigo Canal","Column5", "Tipo Registro", "Moneda", "Tipo Movimiento",
 "Apellido Paterno", "Apellido Materno", "Nombres", "Sexo","Column13", "Fecha de Nacimiento", "Tipo de DOC",
  "Nro de Doc. Identidad","Column17", "Periodicidad", "Columnx", "F .de Afiliación","F. Inicio del seguro", "F. fin de seguro", 
  "Column23","Monto Asegurado.1","Monto Asegurado.2", "Prima.1", "Prima.2", "Columm", "PLAN SM", "Column28","Column29","PLAN BBVA","Column31"]
       csv_buffer.truncate(0)
    csv_buffer.seek(0)
    df2.to_csv(csv_buffer, sep=',',index = None)
    s3_resource.Object(bucket_name, key_name3).put(Body=csv_buffer.getvalue())