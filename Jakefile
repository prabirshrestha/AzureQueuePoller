var 
    fs      = require('fs'),
    path    = require('path'),
    njake   = require('./src/njake'),
    msbuild = njake.msbuild,
    nuget   = njake.nuget,
    assemblyInfo  = njake.assemblyInfo,
    config  = {
        rootPath: __dirname,
        version: fs.readFileSync('VERSION', 'utf-8')
    };

console.log('AzureQueuePoller v' + config.version)

msbuild.setDefaults({
    properties: { Configuration: 'Release' },
    processor: 'x86',
    version: 'net4.0'
})

nuget.setDefaults({
    _exe: 'src/.nuget/NuGet.exe',
    verbose: true
})

task('default', ['build'])

directory('dist/')
directory('working/')

desc('Build')
task('build', ['assemblyInfo'], function () {
    msbuild({
        file: 'src/AzureQueuePoller.sln',
        targets: ['Build']
    })
}, { async: true })

desc('Clean all')
task('clean', function () {
    msbuild({
        file: 'src/AzureQueuePoller.sln',
        targets: ['Clean']
    }, function(code) {
        if (code !== 0) fail('msbuild failed')
        jake.rmRf('bin/')
        jake.rmRf('working/')
        jake.rmRf('dist/')
    })
}, { async: true })

 task('assemblyInfo', function () {
    assemblyInfo({
        file: 'src/AzureQueuePoller/Properties/AssemblyInfo.cs',
        assembly: {
            notice: function () {
                return '// Do not modify this file manually, use jakefile instead.\r\n';
            },
            AssemblyTitle: 'AzureQueuePoller',
            AssemblyDescription: 'Helper utils to poll Windows Azure queue efficiently.',
            AssemblyCompany: '',
            AssemblyProduct: 'AzureQueuePoller',
            AssemblyCopyright: 'Copyright © Prabir Shrestha 2012',
            ComVisible: false,
            AssemblyVersion: config.version,
            AssemblyFileVersion: config.version
        }
    })
    }, { async: true })


namespace('nuget', function () {

    npkgDeps = []

    directory('dist/symbolsource/', ['dist/'])

    nugetNuspecs = fs.readdirSync('src/nuspec').filter(function (nuspec) {
        return nuspec.indexOf('.nuspec') > -1
    })

    symbolsourceNuspecs = fs.readdirSync('src/nuspec/symbolsource')

    namespace('pack', function () {
    
        nugetNuspecs.forEach(function (nuspec) {
            npkgDeps.push('nuget:pack:' + nuspec)
            task(nuspec, ['dist/', 'build'], function () {
                nuget.pack({
                    nuspec: 'src/nuspec/' + nuspec,
                    version: config.version,
                    outputDirectory: 'dist/'
                })
            })
        })

        symbolsourceNuspecs.forEach(function (nuspec) {
            npkgDeps.push('nuget:pack:symbolsource' + nuspec)
            task('symbolsource' + nuspec, ['dist/symbolsource/', 'build'], function () {
                nuget.pack({
                    nuspec: 'src/nuspec/symbolsource/' + nuspec,
                    version: config.version,
                    outputDirectory: 'dist/symbolsource/'
                })
            })
        })

    })

    desc('Create nuget packages')
    task('pack', npkgDeps)

})
