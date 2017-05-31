function clusterMap(map, products, text){
    L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
        {attribution: text, maxZoom: 19}).addTo(map);

    var markers = L.markerClusterGroup();

    for (var i = 1; i < products.length; i++) {
        var product = products[i];

        var marker = L.marker(new L.LatLng(product.latitude, product.longitude))
            .bindPopup(productToString(product));
        markers.addLayer(marker);
    }
    map.addLayer(markers);
}

function productToString(product){
    return ('product: '+product.productId +'<br>'
        +product.typology +' on '+product.operation +'<br>'
        +'T'+product.bedrooms +', '+product.contructedArea +'m2, '+product.bathrooms +' baths');
}
