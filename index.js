var AWS = require('aws-sdk')
var program = require('commander')

program.option('-r, --region [region]', 'AWS region in which operation takes place', 'us-west-2')

program.version('0.0.1')
.command('lv')
.description('List all vaults in my Glacier account')
.action(function(){

  glacier = getClient(program.region)

  glacier.listVaults({accountId: '-'}, function(err, data){
    if(err) console.log(err.stack);
    else printVaultInfo(data['VaultList'])
  })

})

program.parse(process.argv)

function printVaultInfo(vaultList) {
  if(vaultList && vaultList.length > 0){
    var props = [['VaultARN','ARN'], 
    ['VaultName','Name'], ['CreationDate', 'Created On'], 
    ['LastInventoryDate', 'Last Inventory On'], 
    ['NumberOfArchives', '# Archives'], ['SizeInBytes', 'Size (MB)']]
    
    vaultList.forEach(function(vault){
      vault['SizeInBytes'] = Math.round(vault['SizeInBytes']/1048576*100)/100
      props.forEach(function(keyLabel){
        console.log( `${keyLabel[1]}: ${vault[keyLabel[0]]}`)
      })
      logSeperator()
    })

    return
  }

  console.log("There is no vault")
}

function getClient(region){
  console.log(`Selected region: ${region}`)
  logSeperator()
  return new AWS.Glacier({apiVersion: '2012-06-01', region: region});
}

function logSeperator(){
  console.log('----------------------')
}