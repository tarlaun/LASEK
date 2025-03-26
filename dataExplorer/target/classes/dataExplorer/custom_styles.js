function getStyleFromDataset(datasetStyle) {
  const styleOptions = datasetStyle;

  // Select appropriate style function based on style type
  let featureStyleFn;
  switch (styleOptions.type) {
    case 'basic':
      const basicStyle = new ol.style.Style({
        stroke: new ol.style.Stroke({
          color: styleOptions.stroke_color,
          width: styleOptions.stroke_width
        }),
        fill: new ol.style.Fill({
          color: styleOptions.fill_color
        }),
        image: new ol.style.Circle({
          radius: styleOptions.point_radius,
          fill: new ol.style.Fill({ color: styleOptions.fill_color }),
          stroke: new ol.style.Stroke({ color: styleOptions.stroke_color, width: styleOptions.stroke_width })
        })
      });

      // Basic style function
      const basicStyleFn = (feature) => {
        if (styleOptions.label_attribute)
          basicStyle.setText(createTextStyle(feature, styleOptions.label_attribute));
        return basicStyle;
      };
      featureStyleFn = basicStyleFn;
      break;
    case 'categorized':
      // Function to handle categorized styling
      const categorizedStyles = {};
      styleOptions.categories.forEach(category => {
        categorizedStyles[category.value] = new ol.style.Style({
          stroke: new ol.style.Stroke({ color: category.color, width: styleOptions.stroke_width }),
          fill: new ol.style.Fill({ color: category.color }),
          image: new ol.style.Circle({
            radius: styleOptions.point_radius,
            fill: new ol.style.Fill({ color: category.color }),
            stroke: new ol.style.Stroke({ color: styleOptions.stroke_color, width: styleOptions.stroke_width })
          })
        });
      });

      const categorizedStyleFn = (feature) => {
        const featureValue = feature.get(styleOptions.category_attribute);
        const style = categorizedStyles[featureValue] || categorizedStyles['all_others'] || new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'black', width: styleOptions.stroke_width }) });
        if (styleOptions.label_attribute)
          style.setText(createTextStyle(feature, styleOptions.label_attribute));
        return style;
      };

      featureStyleFn = categorizedStyleFn;
      break;
    case 'graduated':
      // Function to handle graduated styling
      const graduatedStyles = [];
      for (let i = 0; i < 10; i++) {
        const t = i / 9; // Scale between 0 and 1
        const color = interpolateColor(t, 0, 1, styleOptions.min_color, styleOptions.max_color);
        graduatedStyles.push(new ol.style.Style({
          stroke: new ol.style.Stroke({ color: color, width: styleOptions.stroke_width }),
          fill: new ol.style.Fill({ color: color }),
          image: new ol.style.Circle({
            radius: styleOptions.point_radius,
            fill: new ol.style.Fill({ color: color }),
            stroke: new ol.style.Stroke({ color: styleOptions.stroke_color, width: styleOptions.stroke_width })
          })
        }));
      }

      const graduatedStyleFn = (feature) => {
        const value = parseFloat(feature.get(styleOptions.graduate_attribute));
        const normalizedValue = (value - styleOptions.min_value) / (styleOptions.max_value - styleOptions.min_value);
        const index = Math.min(Math.floor(normalizedValue * 10), 9);
        const style = graduatedStyles[index];
        if (styleOptions.label_attribute)
          style.setText(createTextStyle(feature, styleOptions.label_attribute));
        return style;
      };
      featureStyleFn = graduatedStyleFn;
      break;

      case 'datetime':
        // 1) Base style = basic stroke/fill
        const dateTimeBaseStyle = new ol.style.Style({ 
          stroke: new ol.style.Stroke({
            color: styleOptions.stroke_color || '#000',
            width: styleOptions.stroke_width || 1
          }),
          fill: new ol.style.Fill({
            color: styleOptions.fill_color || '#fff'
          }),
          image: new ol.style.Circle({
            radius: styleOptions.point_radius || 3,
            fill: new ol.style.Fill({ color: styleOptions.fill_color || '#fff' }),
            stroke: new ol.style.Stroke({
              color: styleOptions.stroke_color || '#000',
              width: styleOptions.stroke_width || 1
            })
          })
        });
      
        const dateTimeStyleFn = (feature) => {
          // Grab the feature’s attribute
          const attrName = styleOptions.selected_datetime_attribute;
          if (attrName) {
            const rawValue = feature.get(attrName);
            if (rawValue) {
              // Parse as date (or use dayjs / moment for more robust parsing)
              const dateVal = new Date(rawValue);
      
              if (!isNaN(dateVal.getTime())) {
                // from_date check
                if (styleOptions.from_date) {
                  const fromDate = new Date(styleOptions.from_date);
                  if (dateVal < fromDate) return null; // Hide feature
                }
                // to_date check
                if (styleOptions.to_date) {
                  const toDate = new Date(styleOptions.to_date);
                  if (dateVal > toDate) return null; // Hide feature
                }
              }
            }
          }
      
          // Label if chosen
          if (styleOptions.label_attribute) {
            dateTimeBaseStyle.setText(createTextStyle(feature, styleOptions.label_attribute));
          }
      
          // Return the style if in range
          return dateTimeBaseStyle;
        };
      
        featureStyleFn = dateTimeStyleFn;
        break;
      

    case 'icon':
      // Function to handle icon styling
      const iconStyleFn = (feature) => {
        return new ol.style.Style({
          image: new ol.style.Icon({ src: styleOptions.icon_url, scale: styleOptions.icon_scale || 1 }),
          text: createTextStyle(feature, styleOptions.label_attribute)
        })
      };
      featureStyleFn = iconStyleFn;
      break;
    default:
      featureStyleFn = basicStyleFn; // Fallback to basic style
  }
  const finalFeatureStyleFn = featureStyleFn;

  const boundaryStyle = new ol.style.Style({
    fill: new ol.style.Fill({ color: styleOptions.stroke_color })
  });

  const interiorStyle = new ol.style.Style({
    fill: new ol.style.Fill({ color: styleOptions.fill_color })
  });

  return (feature) => {
    if (feature.getProperties()["layer"] == "boundary") {
      return boundaryStyle;
    } else if (feature.getProperties()["layer"] == "interior") {
      return interiorStyle;
    } else {
      return finalFeatureStyleFn(feature);
    }
  };
}

// Helper function to create text style for labels
function createTextStyle(feature, labelAttribute) {
  if (labelAttribute && feature.get(labelAttribute)) {
    return new ol.style.Text({
      text: feature.get(labelAttribute).toString(),
      fill: new ol.style.Fill({ color: '#000' }),
      stroke: new ol.style.Stroke({ color: '#fff', width: 2 })
    });
  }
  return null;
}

// Helper function for color interpolation in graduated styling
function interpolateColor(value, minVal, maxVal, minColor, maxColor) {
  // Parse a hex color string to RGB components
  function parseHexColor(hex) {
    if (hex.startsWith("#")) {
      hex = hex.substring(1);
    }
    const r = parseInt(hex.slice(0, 2), 16);
    const g = parseInt(hex.slice(2, 4), 16);
    const b = parseInt(hex.slice(4, 6), 16);
    return [r, g, b];
  }

  // Linear interpolation between two values
  function lerp(start, end, t) {
    return Math.round(start + (end - start) * t);
  }

  // Ensure value is within the range
  const t = (Math.min(Math.max(value, minVal), maxVal) - minVal) / (maxVal - minVal);

  // Parse the hex colors to RGB
  const startRGB = parseHexColor(minColor);
  const endRGB = parseHexColor(maxColor);

  // Interpolate each color component
  const interpolatedRGB = startRGB.map((start, i) => {
    return lerp(start, endRGB[i], t);
  });

  // Convert the RGB array back to a hex color string
  const interpolatedHex = `#${interpolatedRGB.map(x => x.toString(16).padStart(2, '0')).join('')}`;
  return interpolatedHex;
}

function createRandomStyle() {
  // List of 20 distinct colors
  const colors = ['#FF6633', '#FFB399', '#FF33FF', '#FFFF99', '#00B3E6',
    '#E6B333', '#3366E6', '#999966', '#99FF99', '#B34D4D',
    '#80B300', '#809900', '#E6B3B3', '#6680B3', '#66991A',
    '#FF99E6', '#CCFF1A', '#FF1A66', '#E6331A', '#33FFCC'];

  // Check if the dataset already has a style
  // Pick a random color from the colors array
  const randomColor = colors[Math.floor(Math.random() * colors.length)];

  // Return the default style object
  return {
    type: "basic",
    stroke_color: "black",
    stroke_width: 1,
    point_radius: 3,
    fill_color: randomColor,
    opacity: 100,
    visible: true
  };
}