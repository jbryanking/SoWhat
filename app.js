var fs = require('fs'),
	path = require('path'),
	ResourceTimingDecompression = require("resourcetiming-compression").ResourceTimingDecompression,
	gunzip = require('gunzip-file'),
	s3 = require('s3'),
	fileDownloadFolder = "\\\\ctac\\service\\SoastaLogs\\Production\\";//"/Volumes/SostaLogs/Testing/";//Staging/";
	
require('console-stamp')(console, '[HH:MM:ss.l]');

var dateToDownload = (process.argv[2] === "all") ? ("3GR77-9GZPG-JU24Q-3YX9A-SDCF4/") : "3GR77-9GZPG-JU24Q-3YX9A-SDCF4/" + process.argv[2] + '/';
var deleteFilesAfterDownloading = (process.argv[3] === "deleteFilesAfterProcessing");

process.argv.forEach(function (value, index, array) {
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
		Prefix: dateToDownload,//3GR77-9GZPG-JU24Q-3YX9A-SDCF4/",
		Delimiter: '/',
		MaxKeys: 10,
		//Marker: "3GR77-9GZPG-JU24Q-3YX9A-SDCF4\2016-04-16\10.255.138.69\2016-04-16_00_00_05.log.gz"//3GR77-9GZPG-JU24Q-3YX9A-SDCF4/2016-04-16/10.255.138.69/2016-04-16_23_24_01.log.gz"//"3GR77-9GZPG-JU24Q-3YX9A-SDCF4/2016-04-16/10.255.138.69/2016-04-16_23_54_00.log.gz"
		//filter tags?
		// other options supported by getObject 
		// See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property 
	},
};

var doTheNeedful = function () {
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
		console.log("done listing");
		console.log("setting next run time in 3 minutes");
		setTimeout(doTheNeedful, 180000);
	});

	var downloadFile = function (filename) {
		if (filename == undefined || filename == 'undefined') return;
		console.log("checking file: " + filename);
		// if file exists already, drop
		var filepath = fileDownloadFolder + filename;
		fs.stat(filepath, function (err, stats) {
			if (err == undefined) {
				handleDeleteFileFromAmazon(filename);
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
				downloader.localFilepath = filepath;
				downloader.remoteFilepath = filename;
				downloader.on('error', function (err) {
					console.error("unable to download: ", this.remoteFilepath);
					console.error(err.stack);
					if (err.code === "EEXIST") {
						processAndDelete();
					}
				});
				downloader.on('data', function (data) {
					console.log('done with data ' + data);
				});
				downloader.on('end', processAndDelete);


			} else {
				return;
			}
		});
	}

	var processAndDelete = function () {
		console.log("done downloading a file " + this.localFilepath);
		handleDeleteFileFromAmazon(this.remoteFilepath);
		processFile(this.localFilepath);
	};


	var handleDeleteFileFromAmazon = function (remoteFilename) {
		if (deleteFilesAfterDownloading) {
			var s3Params = {
				Bucket: "ctac-soasta",
				Delete: {
					Objects: [
						{ Key: remoteFilename }
					]
				}
			};

			console.log("delete file: " + remoteFilename);
			client.deleteObjects(s3Params);
		}
	}

	var processFile = function (filename) {

		if (filename == undefined || filename == null || filename == "" || filename.length == 0 || filename[0] == '.' || !(filename.substr(filename.length - 3) == ".gz")) return;
		console.log('processing file ' + filename);
		var unzipFilename = filename.substr(0, filename.length - 3);
		try {
			gunzip(filename, unzipFilename, function () {
				setTimeout(hydrateLogs, 500, unzipFilename, filename);
			});
		} catch (err) {
			console.log('\x1b[31m%s\x1b[0m: ', 'error in processFile' + err);
		}
	}
}


var hydrateLogs = function (unzipFilename, filename) {
	var fileContentsArray = fs.readFile(unzipFilename, 'utf8', function (err2, data) {
		if (err2) {
			console.log('\x1b[31m%s\x1b[0m: ', 'error writing hydrated file ' + err);
			return;
		}
		data.split("\n").map(function (x, i) {
			if (x == "" || x.length == 0) return;
			var rt = JSON.parse(x);
			// if (rt.params == undefined || rt.params.restiming == undefined){
			// 	var justFilename = path.basename(unzipFilename);
			// 	var newFilename = path.resolve(path.dirname(unzipFilename), justFilename + '.NoRestimingFound.json')
			// 	 fs.createReadStream(unzipFilename).pipe(fs.createWriteStream(newFilename));
			// 	 return;
			// };
			var original = ResourceTimingDecompression.decompressResources(rt.params.restiming);
			rt.params.restiming = original;
			var hydratedFilename = unzipFilename.substr(0, unzipFilename.length - 4) + '_' + ('000' + i).substr(-3) + '.json';
			// fs.writeFileSync('soastaFiles/' + hydratedFilename + '.json', x, 'utf8');
			fs.writeFile(hydratedFilename, JSON.stringify(rt), 'utf8', function (err) {
				if (err) {
					console.log('\x1b[31m%s\x1b[0m: ', 'error writing hydrated file ' + err);
					return;
				}
				console.log(hydratedFilename + ' has been saved!');
			});
		});
		console.log('File complete: ' + filename);
		//remove unzipFilename
		fs.unlink(unzipFilename, function (err) {
			if (err) {
				console.log('\x1b[31m%s\x1b[0m: ', 'error removing unzip file ' + err);
				return;
			}
			console.log('File removed: ' + unzipFilename);
		});

	});
}
doTheNeedful();