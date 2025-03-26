var DaVinci;
if (typeof DaVinci === "undefined") {
  // Define DaVinci the first time the script is included
  DaVinci = {
    ServerURL: "https://davinci.cs.ucr.edu/",
    VisualizeURL: "https://davinci.cs.ucr.edu/" + "dynamic/visualize.cgi/",

    createDataLayer: function (datasetURL) {
      return new ol.layer.Tile({
        source: new ol.source.XYZ({
          url: DaVinci.VisualizeURL + datasetURL + '/tile-{z}-{x}-{y}.png',
          attributions: 'Powered by <a href="https://davinci.cs.ucr.edu">&copy;DaVinci</a>'
        })
      });
    },
    addLoadEvent: function (func) {
      var oldonload = window.onload;
      if (typeof window.onload != 'function') {
        window.onload = func;
      } else {
        window.onload = function () {
          if (oldonload)
            oldonload();
          func();
        }
      }
    },

    loadScript: function (src) {
      var newScript = document.createElement('script');
      newScript.type = 'text/javascript';
      newScript.src = src;
      document.getElementsByTagName('head')[0].appendChild(newScript);
    },

    loadCSS: function (href) {
      var newStyle = document.createElement('link');
      newStyle.rel = 'stylesheet';
      newStyle.href = href;
      newStyle.type = 'text/css';
      document.getElementsByTagName('head')[0].appendChild(newStyle);
    }

  }
}

// Load OpenLayers if not loaded
if (typeof ol === 'undefined') {
  DaVinci.loadScript("https://cdn.rawgit.com/openlayers/openlayers.github.io/master/en/v5.3.0/build/ol.js");
  DaVinci.loadScript("https://cdn.polyfill.io/v2/polyfill.min.js?features=requestAnimationFrame,Element.prototype.classList,URL");
  DaVinci.loadCSS("https://cdn.rawgit.com/openlayers/openlayers.github.io/master/en/v5.3.0/css/ol.css");
}

(function (scriptElement) {
  DaVinci.addLoadEvent(function () {
    // Locate the parent of the script element to use as the map container
    var mapContainer = scriptElement.parentNode;

    // Extract desired map invofmation
    var dataCenter = scriptElement.getAttribute("data-center");
    var dataZoom = scriptElement.getAttribute("data-zoom");
    var dataBaseLayer = scriptElement.getAttribute("data-base-layer");
    var dataDatasetName = scriptElement.getAttribute("data-dataset-name");
    if (typeof dataDatasetName === "undefined") {
      console.log("DaVinci: Dataset not defined in the script eleemnt. Plase define the attribute 'data-dataset-name'");
      return;
    }
    // Initialize map information
    var latitude = 0.0, longitude = 0.0;
    var zoom = 1;
    var baseLayer = dataBaseLayer || "osm";

    if (dataCenter) {
      var parts = dataCenter.split(',');
      latitude = parseFloat(parts[0]);
      longitude = parseFloat(parts[1]);
    }
    if (dataZoom)
      zoom = parseInt(dataZoom);
    // Now, create the map and intialize as desired
    var view = new ol.View({
      center: ol.proj.fromLonLat([longitude, latitude]),
      zoom: zoom,
      minZoom: 0,
      maxZoom: 19
    });

    // Now initialize the base OSM layer and any user-selected data layers
    // Add the base OSM layer
    const OSMSource = new ol.source.OSM({ crossOrigin: null });
    const GoogleMapsSource = new ol.source.TileImage({
      wrapX: true,
      url: 'https://mt1.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
      attributions: 'Base layer by <a target="_blank" href="https://maps.google.com/">Google Maps</a>'
    });
    const GoogleSatelliteSource = new ol.source.TileImage({
      wrapX: true,
      url: 'https://mt1.google.com/vt/lyrs=s&hl=pl&&x={x}&y={y}&z={z}',
      attributions: 'Base layer by <a target="_blank" href="https://maps.google.com/">Google Maps</a>'
    });
    var source;
    switch (baseLayer) {
      case "osm": source = OSMSource; break;
      case "google-maps": source = GoogleMapsSource; break;
      case "google-satellite": source = GoogleSatelliteSource; break;
      case "none": source = null; break;
      default: source = OSMSource; break;
    }
    var layers = source? [new ol.layer.Tile({ source })] : [];
    layers.push(DaVinci.createDataLayer(dataDatasetName));

    // Now create the map
    var map = new ol.Map({
      target: mapContainer,
      layers: layers,
      view: view
    });
  })
})(document.currentScript);
