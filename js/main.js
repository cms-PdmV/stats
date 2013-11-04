var ListColToDRaw = new Array("", "");

function AddCol(Col)
{
	if(document.ACMTWSmenu.Col.value=="")
	{
		document.ACMTWSmenu.Col.value=document.ACMTWSmenu.Col.value+Col;
	}
	else if(document.ACMTWSmenu.Col.value.indexOf(Col+"-") != -1) 
	{
		document.ACMTWSmenu.Col.value = RemCol(Col+"-");
	}
	else if(document.ACMTWSmenu.Col.value.indexOf(Col) != -1)
	{
		document.ACMTWSmenu.Col.value = RemCol("-"+Col);
	}
	else
	{
		document.ACMTWSmenu.Col.value=document.ACMTWSmenu.Col.value+"-"+Col;
	}
}

function RemCol(Col)
{
	return document.ACMTWSmenu.Col.value.replace(Col,"");
}

Start=0;
function SelectCol(Value) {
	if (ListColToDRaw[0] == "") {
		var Col = document.getElementById("S"+Value);
		Col.style.background = "-ms-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "linear-gradient(to bottom,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-o-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-moz-linear-gradient(top, #7EA4d1, #8EB5E2)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from( #7EA4d1), to( #8EB5E2))";
		ListColToDRaw[0] = Value;
	} else if (ListColToDRaw[0] == Value) {
		var Col = document.getElementById("S"+Value);
		Col.style.background = "-ms-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "linear-gradient(to bottom,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-o-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-moz-linear-gradient(top, #6d93c0, #3d5b78)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from(#6d93c0), to(#3d5b78))";
		ListColToDRaw[0] = ListColToDRaw[1];
		ListColToDRaw[1] = "";
	} else if (ListColToDRaw[1] == "") {
		var Col = document.getElementById("S"+Value);
		Col.style.background = "-ms-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "linear-gradient(to bottom,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-o-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-moz-linear-gradient(top, #7EA4d1, #8EB5E2)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from( #7EA4d1), to( #8EB5E2))";
		ListColToDRaw[1] = Value;

	} else if (ListColToDRaw[1] == Value) {
		var Col = document.getElementById("S"+Value);
		Col.style.background = "-ms-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "linear-gradient(to bottom,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-o-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-moz-linear-gradient(top, #6d93c0, #3d5b78)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from(#6d93c0), to(#3d5b78))";
		ListColToDRaw[1] = "";
	} else {
		var Col = document.getElementById("S"+Value);
		Col.style.background = "-ms-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "linear-gradient(to bottom,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-o-linear-gradient(top,  #7EA4d1 0%,  #8EB5E2 100%)";
		Col.style.background = "-moz-linear-gradient(top, #7EA4d1, #8EB5E2)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from( #7EA4d1), to( #8EB5E2))";
		var Col = document.getElementById(ListColToDRaw[0]);
		Col.style.background = "-ms-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "linear-gradient(to bottom,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-o-linear-gradient(top,  #6d93c0 0%, #3d5b78 100%)";
		Col.style.background = "-moz-linear-gradient(top, #6d93c0, #3d5b78)";
		Col.style.background = "-webkit-gradient(linear, 0 0, 0 100%, from(#6d93c0), to(#3d5b78))";
		ListColToDRaw[0] = ListColToDRaw[1];
		ListColToDRaw[1] = Value;
	}
}

function createGauge(name, label, ValueA) {
	var config = {
		size : 300,
		label : label,
		minorTicks : 5
	}
	config.redZones = [];
	config.redZones.push({
		from : 90,
		to : 100
	});
	config.yellowZones = [];
	config.yellowZones.push({
		from : 75,
		to : 90
	});
	gauges[name] = new Gauge(name, config);
	gauges[name].configure(config);
	gauges[name].render(ValueA);
}

var indice = 0;
var WordToVerify = new Array(71, 82, 65, 80, 72);
function IsGraph(Value) {
	if (Value == WordToVerify[indice]) {
		indice = indice + 1;
		if (indice > 4) {
			indice = 0;
			if (ListColToDRaw[1] != "") {
				document.ACMTWSmenu.Graphic.value = ListColToDRaw[0] + "-" + ListColToDRaw[1];
			} else {
				document.ACMTWSmenu.Graphic.value = ListColToDRaw[0]
			}
			document.ACMTWSmenu.submit();
		}
	} else {
		indice = 0;
	}
}
function SortChangeValue(name) {
	if (document.ACMTWSmenu.SortValue.value == name) {
		if (document.ACMTWSmenu.Order.value == 'False') {
			document.ACMTWSmenu.Order.value = 'True';
		} else {
			document.ACMTWSmenu.Order.value = 'False';
		}
	}
	document.ACMTWSmenu.SortValue.value = name;
	document.ACMTWSmenu.submit();
}

function SortChangeValueB(name, state) {
	document.ACMTWSmenu.Order.value = state;
	document.ACMTWSmenu.SortValue.value = name;
	document.ACMTWSmenu.submit();
}

var gauges = [];
var ContentHeight = 200;
var TimeToSlide = 250.0;
var openAccordion = '';
function runAccordion(index) {
	var nID = "PanelAContent";
	if (openAccordion == nID) {
		nID = '';
	}
	setTimeout("animate(" + new Date().getTime() + "," + TimeToSlide + ",'" + openAccordion + "','" + nID + "')", 33);
	openAccordion = nID;
}

function animate(lastTick, timeLeft, closingId, openingId) {
	var curTick = new Date().getTime();
	var elapsedTicks = curTick - lastTick;
	var opening = (openingId == '') ? null : document.getElementById(openingId);
	var closing = (closingId == '') ? null : document.getElementById(closingId);
	if (timeLeft <= elapsedTicks) {
		if (opening != null)
			opening.style.height = ContentHeight + 'px';
		if (closing != null) {
			closing.style.display = 'none';
			closing.style.height = '0px';
		}
		return;
	}
	timeLeft -= elapsedTicks;
	var newClosedHeight = Math.round((timeLeft / TimeToSlide) * ContentHeight);
	if (opening != null) {
		if (opening.style.display != 'block')
			opening.style.display = 'block';
		opening.style.height = (ContentHeight - newClosedHeight) + 'px';
	}
	if (closing != null)
		closing.style.height = newClosedHeight + 'px';
	setTimeout("animate(" + curTick + "," + timeLeft + ",'" + closingId + "','" + openingId + "')", 33);
}

function Gauge(placeholderName, configuration) {
	this.placeholderName = placeholderName;
	var self = this;
	// some internal d3 functions do not "like" the "this" keyword, hence setting a local variable
	
	this.configure = function(configuration) {
		this.config = configuration;
		this.config.size = this.config.size * 0.9;
		this.config.raduis = this.config.size * 0.97 / 2;
		this.config.cx = this.config.cy = this.config.size / 2;
		this.config.min = 0;
		this.config.max =100;
		this.config.majorTicks = 5;
		this.config.minorTicks = 25;
		this.config.yellowColor =  "#6d93c0";
		this.config.redColor ="#3d5b78";
	}

	this.render = function(ValueA) {
		this.body = d3.select(this.placeholderName).append("svg:svg").attr("class", "gauge").attr("width", this.config.size).attr("height", this.config.size);
		this.body.append("svg:circle").attr("cx", this.config.cx).attr("cy", this.config.cy).attr("r", this.config.raduis).style("fill", "#ccc").style("stroke", "#a0a0a0").style("stroke-width", "1px");
		this.body.append("svg:circle").attr("cx", this.config.cx).attr("cy", this.config.cy).attr("r", 0.9 * this.config.raduis).style("fill", "#fff").style("stroke", "#e0e0e0").style("stroke-width", "1px");
		this.drawBand(this.config.yellowZones[0].from, this.config.yellowZones[0].to, self.config.yellowColor);
		this.drawBand(this.config.redZones[0].from, this.config.redZones[0].to, self.config.redColor);

		var fontSize = Math.round(this.config.size / 9);
		this.body.append("svg:text").attr("x", this.config.cx).attr("y", this.config.cy / 2 + fontSize / 2).attr("dy", fontSize / 2).attr("text-anchor", "middle").text(this.config.label).style("font-size", fontSize / 2 + "px").style("fill", "#333").style("stroke-width", "0px");
		var fontSize = Math.round(this.config.size / 16);
		var majorDelta = this.config.max / (this.config.majorTicks - 1);
		for (var major = this.config.min; major <= this.config.max; major += majorDelta) {
			var minorDelta = majorDelta / this.config.minorTicks;
			for (var minor = major + minorDelta; minor < Math.min(major + majorDelta, this.config.max); minor += minorDelta) {
				var point1 = this.valueToPoint(minor, 0.75);
				var point2 = this.valueToPoint(minor, 0.85);
				this.body.append("svg:line").attr("x1", point1.x).attr("y1", point1.y).attr("x2", point2.x).attr("y2", point2.y).style("stroke", "#FFF").style("stroke-width", "1px").transition().duration(minor*50).style("stroke", "#666");
			}
			var point1 = this.valueToPoint(major, 0.7);
			var point2 = this.valueToPoint(major, 0.85);
			this.body.append("svg:line").attr("x1", point1.x).attr("y1", point1.y).attr("x2", point2.x).attr("y2", point2.y).style("stroke", "#333").style("stroke-width", "2px");
			if (major == this.config.min || major) {
				var point = this.valueToPoint(major, 0.63);
				this.body.append("svg:text").attr("x", point.x).attr("y", point.y).attr("dy", fontSize / 3).attr("text-anchor", major == this.config.min ? "start" : "end").text(major).style("font-size", fontSize + "px").style("fill", "#333").style("stroke-width", "0px");
			}
		}
		var pointerContainer = this.body.append("svg:g").attr("class", "pointerContainer");
		this.drawPointer(ValueA);
		pointerContainer.append("svg:circle").attr("cx", this.config.cx).attr("cy", this.config.cy).attr("r", 0.12 * this.config.raduis).style("fill", "#6d93c0").style("stroke", "#7eA4D1").style("opacity", 1);
	}
	this.drawBand = function(start, end, color) {
		if (0 >= end - start)
			return;
		this.body.append("svg:path").style("fill", "#fff").attr("d", d3.svg.arc().startAngle(this.valueToRadians(start)).endAngle(this.valueToRadians(end)).innerRadius(0.65 * this.config.raduis).outerRadius(0.85 * this.config.raduis)).attr("transform", function() {
			return "translate(" + self.config.cx + ", " + self.config.cy + ") rotate(270)"
		}).transition().duration(4000).style("fill", color);
	}
	this.drawPointer = function(value) {
		var delta = this.config.max / 13;
		var head = this.valueToPoint(value, 0.85);
		var head1 = this.valueToPoint(value - delta, 0.12);
		var head2 = this.valueToPoint(value + delta, 0.12);
		var tailValue = value - (this.config.max * (1 / (270 / 360)) / 2);
		var tail = this.valueToPoint(tailValue, 0.28);
		var tail1 = this.valueToPoint(tailValue - delta, 0.12);
		var tail2 = this.valueToPoint(tailValue + delta, 0.12);
		var data = [head, head1, tail2, tail, tail1, head2, head];
		var line = d3.svg.line().x(function(d) {
			return d.x
		}).y(function(d) {
			return d.y
		}).interpolate("basis");
		this.body.select(".pointerContainer").selectAll("path").data([data]).enter().append("svg:path").attr("d", line).style("fill", "#777777").style("stroke", "#DDDDDD").style("fill-opacity", 0.7)
		var fontSize=Math.round(this.config.size / 10);
		this.body.select(".pointerContainer").selectAll("text").data([value]).text(value).enter().append("svg:text").attr("x", this.config.cx).attr("y", this.config.size - this.config.cy / 4 - fontSize).attr("dy", fontSize / 2).attr("text-anchor", "middle").text(Math.round(value)).style("font-size", fontSize + "px").style("fill", "#000").style("stroke-width", "0px");
	}
	this.valueToRadians = function(value) {
		return (value / this.config.max * 270 - 45) * Math.PI / 180;
	}
	this.valueToPoint = function(value, factor) {
		var point = {
			x : this.config.cx - this.config.raduis * factor * Math.cos(this.valueToRadians(value)),
			y : this.config.cy - this.config.raduis * factor * Math.sin(this.valueToRadians(value))
		}
		return point;
	}
	// initialization
	this.configure(configuration);
}

function GetSizeWindow() {
	var Size = new Array()
	if ( typeof (window.innerWidth ) == 'number') {
		//Non-IE
		Size[0] = window.innerWidth;
		Size[1] = window.innerHeight;
	} else if (document.documentElement && (document.documentElement.clientWidth || document.documentElement.clientHeight )) {
		//IE 6+ in 'standards compliant mode'
		Size[0] = document.documentElement.clientWidth;
		Size[1] = document.documentElement.clientHeight;
	} else if (document.body && (document.body.clientWidth || document.body.clientHeight )) {
		//IE 4 compatible
		Size[0] = document.body.clientWidth;
		Size[1] = document.body.clientHeight;
	}
	return Size;
}

function ScatterPlotC(Data,ScaleA,ScaleB,ContainerBalise,widthA,heightA,Label)
{
		
		MaxValueX = 0;
		MaxValueY = 0;
		MaxValueZ = 0;
		LetterX = 0;
		LetterY=0;
		for ( i = 0; i < Data.length; i++) {
			if (Data[i][1] > MaxValueY) {
				MaxValueY = Data[i][1];
			}
			if (Data[i][2] > MaxValueZ) {
				MaxValueZ = Data[i][2];
			}
			if (Data[i][0] > MaxValueX) {
				MaxValueX = Data[i][0];
			}
		}
		
	  tailleA=0;
	  if(ScaleA.length==0)
	  {
	  	Xvalue=new Array()
			i=0;
			count = 0;
			while(i<Data.length)
			{
				if(Xvalue.indexOf(Data[i][0])==-1)
				{
					Xvalue.push(Data[i][0]);
					count=count+1;
					
				}
				
				i=i+1;
			}
			MaxValueX=count;
			LetterX=1;
	  
	  	tailleA=Xvalue.length +1
	  }
	  else
	  	tailleA=ScaleA.length
	  	
	  	
	  TailleB=0;
	  if(ScaleB.length==0)
	  {
	  Yvalue=new Array();
			i=0;
			count = 0;
			while(i<Data.length)
			{
				if(Yvalue.indexOf(Data[i][1])==-1)
				{
					Yvalue.push(Data[i][1]);
					count=count+1;
					
				}
				
				i=i+1;
			}
				
			MaxValueY=count;
			LetterY=1;
	  	tailleB=Yvalue.length +1 
	  }
	  else
	  	tailleB=ScaleB.length
	  	
   		var OverflowX = 100;
   		var OverflowY = 40;
		var Vx = (widthA-OverflowX-15)/(tailleA-1);
		var Vy = (heightA-OverflowY-15)/(tailleB-1);
		var Vz = (1/MaxValueZ);
		var chart = d3.select(ContainerBalise).append("svg").attr("class", "chart").attr("width", widthA).attr("height", heightA);
		chart.append("line").attr("x1", OverflowX).attr("x2", widthA - 15).attr("y1", heightA -  OverflowY).attr("y2", heightA - OverflowY).style("stroke", "#000");
		chart.append("line").attr("x1", OverflowX).attr("x2",OverflowX).attr("y1", heightA -OverflowY).attr("y2", +15 ).style("stroke", "#000");
		
		
	  for(i=0;i<Data.length;i++)
	  {	
	  	chart.append("circle")
      	.attr("class", "node")
      	.attr("cx", function(){
      		if(LetterX==0)
      			return Vx*Data[i][0]+OverflowX+Vx/2;
      		else
      			return Vx*Xvalue.indexOf(Data[i][0])+OverflowX+Vx/2;
      		})
      	.attr("cy", function(){
      		if(LetterY==0)
      			return heightA-Vy*Data[i][1]-OverflowY-Vy/2;
      		else
      			return heightA-Vy*Yvalue.indexOf(Data[i][1])-OverflowY-Vy/2;
      		})
      	.attr("r", function(){
      		if(Vx>Vy)
      			return Vy/2;
      		else
      			return Vx/2;
      	})
      	.style("fill", "white")
      	.transition()
        .duration(500+(1000/Data.length*i))
      	.style("fill", "steelblue")
      	.style("opacity",Vz*Data[i][2]+0.05);
	  }
	  
	  
	  
	  
	  for ( i = 0; i < tailleA ; i++) {
	  	if(LetterX==0)
	  	{
			chart.append("line").attr("x1", Vx*i + OverflowX).attr("x2", Vx*i + OverflowX).attr("y1", heightA - OverflowY).attr("y2", heightA - OverflowY+3).style("stroke", "#000");
			chart.append("svg:text").attr("x",  Vx*i + OverflowX).attr("y", function(d) {
				if ((ScaleA[i] > 10000) && (i % 2 == 1)) {
					return heightA - OverflowY+18;
				} else {
					return heightA - OverflowY+8;
				}
			}).attr("dy", ".35em").style("font", "10px Arial")
			.style("fill", "white")
			.transition()
       		.duration(500+(1000/tailleA*i))
       		.style("fill", "black")
       		.attr("text-anchor", "middle").text(String(ScaleA[i]));
		}
		else
		{
			chart.append("line").attr("x1", Vx*i + OverflowX +Vx/2).attr("x2", Vx*i + OverflowX+Vx/2).attr("y1", heightA - OverflowY).attr("y2", heightA - OverflowY+3).style("stroke", "#000");
			chart.append("svg:text").attr("x",  Vx*i + OverflowX+Vx/2).attr("y", function(d) {
				if ((i % 2 == 1)) {
					return heightA - OverflowY+18;
				} else {
					return heightA - OverflowY+8;
				}
			}).attr("dy", ".35em").style("font", "10px Arial")
			.style("fill", "white")
			.transition()
       		.duration(500+((1000/(tailleA))*i))
       		.style("fill", "black")
       		.attr("text-anchor", "middle").text(String(Xvalue[i]));
		}
		}
	for ( i = 0; i < tailleB ; i++) {
		if(LetterY==0)
		{
			chart.append("line").attr("x1",  OverflowX-3).attr("x2",  OverflowX).attr("y1", heightA - OverflowY -Vy * i ).attr("y2", heightA - OverflowY-Vy*i).style("stroke", "#000");
			chart.append("svg:text")
			.attr("y",  heightA - OverflowY - Vy*i )
			.style("fill", "white")
			.attr("x",  function(){
				if(String(ScaleB[i]).length<=17)
				{
					return OverflowX-String(ScaleB[i]).length*6 -5;
				}
				else
				{
					return OverflowX-100;}})
			.style("font", "10px Arial ")
			.transition()
       		.duration(500+(1000/(tailleB)*i))
       		.style("fill", "black")
       		.attr("dy", ".35em").attr("text-anchor", "right").text(String(ScaleB[i]));
       	}
       	else
       	{
       		chart.append("line").attr("x1",  OverflowX-3).attr("x2",  OverflowX).attr("y1", heightA - OverflowY -Vy * i -Vy/2 ).attr("y2", heightA - OverflowY-Vy*i -Vy/2).style("stroke", "#000");
			chart.append("svg:text")
			.attr("y",  heightA - OverflowY - Vy*i -Vy/2 )
			.style("fill", "white")
			.attr("x",  function(){
				if(String(Yvalue[i]).length<=17)
				{
					return OverflowX-String(Yvalue[i]).length*6 -5;
				}
				else
				{
					return OverflowX-100;}} )
			.style("font", "10px Arial ")
			.transition()
       		.duration(500+(1000/(tailleB)*i))
       		.style("fill", "black")
       		.attr("dy", ".35em").attr("text-anchor", "right").text(String(Yvalue[i]));
       	}
		}
		
		chart.append("svg:text").attr("x", widthA / 2).attr("y", heightA - 8).attr("dy", ".35em").attr("text-anchor", "middle").text(Label);
	  
}
function HistogramC(valuesA, ScaleA, ContainerBalise, widthA, heightA, Label) {
	MaxValuesA = 0;

	if (ScaleA.length == valuesA.length) {
		valuesA = SubTable(valuesA,Start,Start+6);
		ScaleA = SubTable(ScaleA,Start,Start+6);
		for ( i = 0; i < valuesA.length; i++) {
			if (valuesA[i] > MaxValuesA) {
				MaxValuesA = valuesA[i];
			}
		}
		var x = d3.scale.linear().domain([0, valuesA.length]).range([0, widthA - 50]);
		var y = d3.scale.linear().domain([0, MaxValuesA]).range([0, heightA - 60]);
		var chart = d3.select(ContainerBalise).append("svg").attr("class", "chart").attr("width", widthA).attr("height", heightA);
		
		chart.selectAll("rect").data(valuesA).enter().append("rect")
		.attr("y", function(d) {
			return heightA - y(d) - .5 - 40;
			
		})
		.attr("width", function(d, i) {
			return x(1);
		})
		 .attr("x", function(d, i) {
			return x(i) + 0.5 + 15;
		})
		.style("font", "11px serif")
			.style("fill" , "#EFEFEF")
		.style("stroke", "#EFEFEF")
		
		.attr("height", function(d) {
			return y(d);
		})
		.transition()
        .duration(function(d, i) {
			return x(i)*8 ;
		})
        .style("stroke", "#EFEFEF")
		.style("fill" , "steelblue");
		
		chart.selectAll("text").data(valuesA).enter().append("text").attr("x", function(d, i) {
			return x(i) + x(1) / 2 + 15;
		}).attr("width", function(d, i) {
			return x(1);
		})
		.transition()
       .duration(1500)
       .attr("y", function(d) {

			return heightA - y(d) - .5 - 40;
		})
		.attr("height", function(d) {
			return y(d);
		})
		.attr("text-anchor", "middle").style("font-size", 10).text(String);
		for ( i = 0; i < ScaleA.length; i++) {
			chart.append("svg:text").attr("x", x(1) / 2 + x(i) + 15).attr("y", function(d) {
				if (i % 2 == 1) {
					return heightA - .5 - 22;
				} else {
					return heightA - 32;
				}
			}).attr("dy", ".35em").style("font", "10px Arial").attr("text-anchor", "middle").text(String(ScaleA[i]));
		}
		for ( i = 0; i < valuesA.length + 1; i++) {
			chart.append("line").attr("x1", x(i) + 15).attr("x2", x(i) + 15).attr("y1", heightA - 40).attr("y2", heightA - 37).style("stroke", "#000");
		}
		chart.append("line").attr("x1", +15).attr("x2", widthA - 35).attr("y1", heightA - .5 - 40).attr("y2", heightA - .5 - 40).style("stroke", "#000");
		chart.append("svg:text").attr("x", widthA / 2).attr("y", heightA - 8).attr("dy", ".35em").attr("text-anchor", "middle").text(Label);
	} else if (ScaleA.length > valuesA.length) {
		for ( i = 0; i < valuesA.length; i++) {
			if (valuesA[i] > MaxValuesA) {
				MaxValuesA = valuesA[i];
			}
		}
   
		var x = d3.scale.linear().domain([0, valuesA.length]).range([0, widthA - 50]);
		var y = d3.scale.linear().domain([0, MaxValuesA]).range([0, heightA - 60]);
		var chart = d3.select(ContainerBalise).append("svg").attr("class", "chart").attr("width", widthA).attr("height", heightA);
		chart.selectAll("rect").data(valuesA).enter().append("rect").attr("x", function(d, i) {
			return x(i) + 0.5 + 15;
		}).attr("y", function(d) {
			return heightA - y(d) - .5 - 40;
		}).attr("width", function(d, i) {
			return x(1);
		})
		.style("fill" , "#EFEFEF")
		.style("stroke", "#EFEFEF")
		
		.attr("height", function(d) {
			return y(d);
		})
		.transition()
        .duration(function(d, i) {
			return x(i)*5 ;
		})
        .style("stroke", "#EFEFEF")
		.style("fill" , "steelblue");
		
		chart.selectAll("text").data(valuesA).enter().append("text").attr("x", function(d, i) {
			return x(i) + x(1) / 2 + 15;
		})
		.style("font", "11px serif")
		.transition()
       .duration(function(d, i) {
			return x(i) *5;
		}).attr("y", function(d) {

			return heightA - y(d) - .5 - 40;

		}).attr("width", function(d, i) {
			return x(1);
		})
		
		.attr("height", function(d) {
			return y(d);
		}).attr("text-anchor", "middle").style("font-size", 10).text(String);
		for ( i = 0; i < ScaleA.length; i++) {
			chart.append("svg:text").attr("x", x(i) + 15).attr("y", function(d) {
				if ((ScaleA[i] > 10000) && (i % 2 == 1)) {
					return heightA - .5 - 22;
				} else {
					return heightA - 32;
				}
			}).attr("dy", ".35em").style("font", "10px Arial").attr("text-anchor", "middle").text(String(ScaleA[i]));
		}
		for ( i = 0; i < valuesA.length + 1; i++) {
			chart.append("line").attr("x1", x(i) + 15).attr("x2", x(i) + 15).attr("y1", heightA - 40).attr("y2", heightA - 37).style("stroke", "#000");
		}

		chart.append("line").attr("x1", +15).attr("x2", widthA - 35).attr("y1", heightA - .5 - 40).attr("y2", heightA - .5 - 40).style("stroke", "#000");
		chart.append("svg:text").attr("x", widthA / 2).attr("y", heightA - 8).attr("dy", ".35em").attr("text-anchor", "middle").text(Label);
	}
}


function SubTable(TableA,Start,End)
{
	TableB = new Array();
	i=0;
	for(i=Start;i<End;i++)
	{
		if(TableA[i]!=undefined)
		{
			TableB.push(TableA[i]);
		}
	}	
	return TableB;
}
