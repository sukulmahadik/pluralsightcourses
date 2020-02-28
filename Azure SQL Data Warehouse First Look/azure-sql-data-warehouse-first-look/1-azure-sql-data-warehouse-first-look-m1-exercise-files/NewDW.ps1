#setup your account
Login-AzureRmAccount

#verify your subscriptions
Get-AzureRmSubscription

#set your default Subscription
Get-AzureRmSubscription `
–SubscriptionName “Visual Studio Premium with MSDN” | Select-AzureRmSubscription

#create a new datawarehouse
New-AzureRmSqlDatabase -RequestedServiceObjectiveName "DW100" `
-DatabaseName "warnerwarehousePShell" `
-ServerName "warnerdatawarehouses" `
-ResourceGroupName "warehouses" `
-Edition "DataWarehouse"