
/*Se quiere averiguar la media de los beneficios sacados en los años
2018 y 2020*/



db.ventas.aggregate([{$match: { 
    $or: [
        {Fecha : { $lte: new ISODate("2019-01-01")}},
        {Fecha : { $gte: new ISODate("2020-01-01")}}]}},

{$group: {
    _id: {year: { $year: "$Fecha" }},
    media: {$avg: {$multiply:["$Precio €", "$Nº UD"]}},
    
}}
])

/*
{ "_id" : { "year" : 2018 }, "media" : 2790 }
{ "_id" : { "year" : 2020 }, "media" : 1881.6666666666667 }
*/



/*Queremos premiar a los clientes Premium con un descuento extra del 
15% */

db.ventas.aggregate([
    {
        $match:
            {"Premium": true}},
            {$group:{
                _id: {Cliente : "$Cliente"}
            }},
                
            {$project:{
            item:1,
            Cliente:1,
            Descuento:
            { $ifNull: [ "$Descuento", 15 ] }
          }
     },
    
  ]
)           

/*{ "_id" : { "Cliente" : "004" }, "Descuento" : 15 }
{ "_id" : { "Cliente" : "001" }, "Descuento" : 15 }
{ "_id" : { "Cliente" : "003" }, "Descuento" : 15 }*/




/*Se quiere saber el registro detallado de las ventas realizadas en 
2019 y en que momento se hicieron*/

db.ventas.aggregate([{$match: {"Fecha": { 
    $gte: new ISODate("2019-01-01"), 
    $lt: new ISODate("2020-01-01")
}
}},
{$group: {
    _id: {
        month: { $month: "$Fecha" },
        day: { $dayOfMonth: "$Fecha" },
        year: { $year: "$Fecha" }
    },
    totalSales: {$sum:{$multiply:["$Precio €", "$Nº UD"]}}}},
])


/*
{ "_id" : { "month" : 8, "day" : 23, "year" : 2019 }, "totalSales" : 3156 }
{ "_id" : { "month" : 8, "day" : 19, "year" : 2019 }, "totalSales" : 1550 }
{ "_id" : { "month" : 1, "day" : 25, "year" : 2019 }, "totalSales" : 1932 }
{ "_id" : { "month" : 8, "day" : 8, "year" : 2019 }, "totalSales" : 1989 }
{ "_id" : { "month" : 8, "day" : 12, "year" : 2019 }, "totalSales" : 3260 }
{ "_id" : { "month" : 5, "day" : 23, "year" : 2019 }, "totalSales" : 3330 }
{ "_id" : { "month" : 5, "day" : 21, "year" : 2019 }, "totalSales" : 7658 }*/



/* Se quiere saber que mes ha sido el mas fructifero desde la apertura
de la empresa*/


db.ventas.aggregate(
    [
      {
        $group:{
            _id: { month: { $month: "$Fecha"}, year: { $year: "$Fecha" }},
            Ventas:{ $addToSet: "$_id" }}},
        
        {
            $sort:{_id:-1}
        }])

/*{ "_id" : { "month" : 12, "year" : 2018 }, "Ventas" : [ 20 ] }
{ "_id" : { "month" : 11, "year" : 2020 }, "Ventas" : [ 18 ] }
{ "_id" : { "month" : 9, "year" : 2020 }, "Ventas" : [ 16 ] }
{ "_id" : { "month" : 8, "year" : 2020 }, "Ventas" : [ 8, 1, 9, 12, 3 ] }
{ "_id" : { "month" : 8, "year" : 2019 }, "Ventas" : [ 17, 19, 2, 10 ] }
{ "_id" : { "month" : 8, "year" : 2018 }, "Ventas" : [ 5, 21, 15 ] }
{ "_id" : { "month" : 7, "year" : 2020 }, "Ventas" : [ 14 ] }
{ "_id" : { "month" : 5, "year" : 2019 }, "Ventas" : [ 7, 13, 4 ] }
{ "_id" : { "month" : 4, "year" : 2020 }, "Ventas" : [ 11 ] }
{ "_id" : { "month" : 1, "year" : 2019 }, "Ventas" : [ 6  ] }*/

/*Al igual se puede saber el año */

db.ventas.aggregate(
    [
      {
        $group:{
            _id: {  year: { $year: "$Fecha" }},
            Ventas:{ $addToSet: "$_id" }}},
        
        {
            $sort:{_id:-1}
        }])

/*
{ "_id" : { "year" : 2020 }, "Ventas" : [ 1, 9, 18, 11, 14, 12, 3, 16, 8 ] }
{ "_id" : { "year" : 2019 }, "Ventas" : [ 4, 6, 10, 19, 13, 2, 7, 17 ] }
{ "_id" : { "year" : 2018 }, "Ventas" : [ 20, 15, 21, 5 ] }*/

/*LA EMPRESA HA AUMENTADO SUS VENTAS AÑO TRAS AÑO*/



/*ANÁLISIS DE PRODUCTOS*/

/*Para saber que tipo de Nevera se vende mas, se va a 
visualizar el pedido maximo de cada uno de ellos, y cuantos elementos han comprado
como maximo por pedido */

db.ventas.aggregate(
    [
      {
        $group:
          {
            _id: "$Tipo",
            maxPedido: { $max: { $multiply: [ 
                "$Precio €", 
                "$Nº UD" ] 
            }
            },
            maxCantidadXpedido: { $max: "$Nº UD" }
          }
      },

    ])

/*{ "_id" : "B", "maxPedido" : 3540, "maxCantidadXpedido" : 5 }
{ "_id" : "D", "maxPedido" : 3829, "maxCantidadXpedido" : 7 }
{ "_id" : "A", "maxPedido" : 4560, "maxCantidadXpedido" : 9 }
{ "_id" : "C", "maxPedido" : 1390, "maxCantidadXpedido" : 2 }*/



/*los beneficios que ha reportado cada tipo de nevera*/

db.ventas.aggregate(
    [ {
        $group: {
            _id: "$Tipo",
            Total:{$sum:{ $multiply: [ "$Precio €", "$Nº UD" ] }}
        }},
        ])
        

/*{ "_id" : "B", "Total" : 13974 }
{ "_id" : "D", "Total" : 12768 }
{ "_id" : "C", "Total" : 2092 }
{ "_id" : "A", "Total" : 19411 }*/


/*Dado que el Tipo A es el que más se vende, vamos a ver qué artículo dentro
del segmento es reporta más beneficios*/


db.ventas.aggregate(
    [{
        $match:{"Tipo": "A"}
    },
    {
    $group: {
        _id: "$Fabricante",
        MejorProducto: {$max:"$Precio €"}
    }}
    
        ])
/*{ "_id" : "Zanussi", "MejorProducto" : 555 }
{ "_id" : "Samnsung", "MejorProducto" : 865 }
{ "_id" : "Bosch", "MejorProducto" : 912 }*/



/*Además de saber que marca es la mas popular en el último año, debido al
número de pedidos*/

db.ventas.aggregate(
    [
        {$match:{Fecha: { $gte: new Date("2020-01-01")}}},
    {
    $group: {
        _id: "$Fabricante",
        MasVendido: {$max:"$Nº UD"}
    }},  ])
/*
        { "_id" : "Zanussi", "MasVendido" : 2 }
        { "_id" : "Samnsung", "MasVendido" : 9 }
        { "_id" : "Bosch", "MasVendido" : 2 }*/




/* ANÁLISIS DE CLIENTES*/

/*Se quiere saber cual ha sido el desembolso máximo de cada cliente por pedido y 
cuantos pedidos ha realizado desde el inicio*/
 db.ventas.aggregate(
    [
      {
        $group:
          {
            _id: "$Cliente",
            maxPedido: { $max: { $multiply: [ "$Precio €", "$Nº UD" ] } },
            maxCantidad: { $max: "$Nº UD" }
          }
      },

    ]
 )
 /*{ "_id" : "004", "maxPedido" : 3156, "maxCantidad" : 4 }
{ "_id" : "001", "maxPedido" : 4560, "maxCantidad" : 9 }
{ "_id" : "002", "maxPedido" : 3829, "maxCantidad" : 7 }
{ "_id" : "003", "maxPedido" : 3540, "maxCantidad" : 4 }*/


/*Se quiere saber cual ha sido la media de pedidos realizada por cada cliente
 desde el inicio*/

db.ventas.aggregate(
    [{
        $group: {
            _id: "$Cliente",
            total: {$sum: "$Nº UD"},
            count: {$sum: 1},
        }
    },
    {$project:{
        total:1,
        count: 1,
        Promedio_pedido: {$divide:["$total","$count"]}
    }}
])

/*{ "_id" : "004", "total" : 10, "count" : 4, "Promedio_pedido" : 2.5 }
{ "_id" : "001", "total" : 33, "count" : 8, "Promedio_pedido" : 4.125 }
{ "_id" : "002", "total" : 26, "count" : 5, "Promedio_pedido" : 5.2 }
{ "_id" : "003", "total" : 10, "count" : 4, "Promedio_pedido" : 2.5 }*/




/*Se quiere saber cuantos pedidos ha realizado cada transportista en el último año,
si se hace menos de los acordados por contrato, la empresa recibe una
penalización*/

db.ventas.aggregate(
    [{
        $match:{Fecha : { $gte: new Date("2020-01-01")}}},
        
        {
        $group: {
            _id: "$Transporte",
            total: {$sum: "$Nº UD"},
        }},
    {$project:
        {
        item:1,
        Fecha: {$year: new Date("2020-01-01")  },
        total:1,
        Penalizacion:{
            $cond: {if : {$lte: ["$total",10]}, 
                        then: true, else: false}
        }
    }}
])

/*
{ "_id" : "SEUR", "total" : 18, "Fecha" : 2020, "Penalizacion" : false }
{ "_id" : "MRW", "total" : 4, "Fecha" : 2020, "Penalizacion" : true }
{ "_id" : "NACEX", "total" : 2, "Fecha" : 2020, "Penalizacion" : true }
*/
