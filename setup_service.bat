
call nssm.exe install mis_raw_outages_service "%cd%\run_server.bat"
call nssm.exe set mis_raw_outages_service AppStdout "%cd%\logs\mis_raw_outages_service.log"
call nssm.exe set mis_raw_outages_service AppStderr "%cd%\logs\mis_raw_outages_service.log"
call sc start mis_raw_outages_service
rem call nssm.exe edit mis_raw_outages_service