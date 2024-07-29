function popMapLegend() {
  var map_opt = document.getElementById("map_opt");
  var kaart = document.getElementById("kaart");
	if(map_opt.style.opacity < "0.1"){
		map_opt.style.opacity = "1";
		}
	else if(map_opt.style.display == "none" ) {
		map_opt.style.display = "block";
	  }
	else {
		map_opt.style.display = "none";
	  }
  }
