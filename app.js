var fs = require('fs'),
	ResourceTimingDecompression = require("resourcetiming-compression").ResourceTimingDecompression,
	gunzip = require('gunzip-file'),
	s3 = require('s3'),
	fileDownloadFolder = "/Volumes/SostaLogs/Testing/";//Staging/";

var dateToDownload = process.argv[2];

process.argv.forEach(function(value, index, array){
	console.log(index + ":" + value);
});

var client = s3.createClient({
	maxAsyncS3: 3,     // this is the default 
	s3RetryCount: 3,    // this is the default 
	s3RetryDelay: 1000, // this is the default 
	multipartUploadThreshold: 20971520, // this is the default (20 MB) 
	multipartUploadSize: 15728640, // this is the default (15 MB) 
	s3Options: {
		accessKeyId: "AKIAJ4EKU5N4IFTXH6RQ",
		secretAccessKey: "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",
		region: 'us-east-1'
		// any other options are passed to new AWS.S3() 
		// See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property 
	},
});

var params = {
	recursive: true,
	s3Params: {
		Bucket: "ctac-soasta",
		Prefix: "3GR77-9GZPG-JU24Q-3YX9A-SDCF4/" + dateToDownload + '/',//3GR77-9GZPG-JU24Q-3YX9A-SDCF4/",
		Delimiter: '/',
		MaxKeys: 10
		// other options supported by getObject 
		// See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property 
	},
};

var lister = client.listObjects(params);
lister.on('error', function (err) {
	console.error("unable to get:", err.stack);
});
// lister.on('progress', function () {
// 	console.log("progress", lister.progressAmount, lister.progressTotal);
// });
lister.on('data', function (data) {
	// console.log('adding ' + data.Contents.map(function(element) {return '\n' + element.Key}) + '\nto download queue');
	downloadFile(data.Contents.forEach(function (element) {
		if (element.Key == undefined || element.Key == "") {
			// console.log('key was undefined or empty');
			return;
		}
		setTimeout(downloadFile, 500, element.Key); 
	}));
})
lister.on('end', function () {
	console.log("done downloading");
});

var downloadFile = function (filename) {
	if (filename == undefined || filename == 'undefined') return;
	console.log("checking file: " + filename);
	// if file exists already, drop
	var filepath = fileDownloadFolder + filename;
	fs.stat(filepath, function (err, stats) {
		if (err == undefined) {
			return; //skipping file
		} else if (err.code == 'ENOENT') {
			// file does not exist
			console.log('downloading file to: ' + filepath);
			var fileParams = {
				localFile: filepath,

				s3Params: {
					Bucket: "ctac-soasta",
					Key: filename
				},
			};
			//download the file
			//on completion call process
			var downloader = client.downloadFile(fileParams);
			downloader.filename = filepath;
			downloader.on('error', function (err) {
				console.error("unable to download:", err.stack);
			});
			downloader.on('data', function (data) {
				console.log('done with data ' + data);
			});
			downloader.on('end', function () {
				console.log("done downloading a file " + this.filename);
				processFile(this.filename);
			});


		} else {
			return;
		}
	});
}


var processFile = function (filename) {

	if (filename == null || filename == "" || filename.length == 0 || filename[0] == '.' || !filename.endsWith(".gz")) return;
	console.log('processing file ' + filename);
	var unzipFilename = filename.substr(0, filename.length - 3);
	gunzip(filename, unzipFilename, function () {

		var fileContentsArray = fs.readFile(unzipFilename, 'utf8', function (err2, data) {
			if (err2) throw err2;
			data.split("\n").map(function (x, i) {
				if (x == "" || x.length == 0) return;
				var rt = JSON.parse(x);
				if (rt.params == undefined || rt.params.restiming == undefined) return;
				var original = ResourceTimingDecompression.decompressResources(rt.params.restiming);
				rt.params.restiming = original;
				var hydratedFilename = unzipFilename.substr(0, unzipFilename.length - 4) + '_' + ('000' + i).substr(-3) + '.json';
				// fs.writeFileSync('soastaFiles/' + hydratedFilename + '.json', x, 'utf8');
				fs.writeFile(hydratedFilename, JSON.stringify(rt), 'utf8', (err) => {
					if (err) throw err;
					console.log(hydratedFilename + ' has been saved!');
				});
			});

			//remove unzipFilename
			fs.unlink(unzipFilename, (err) => {
				if (err) console.log('\x1b[31m%s\x1b[0m: ', 'error removing unzip file ' + err);
				console.log('File removed: ' + unzipFilename);
			});
			//make zipfile tiny
			fs.writeFile(filename, 'Processed', (err) => {
				if (err) throw err;
				console.log('File complete: ' + filename);
			});
		});
	});
}