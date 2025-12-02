[Setup]
AppId={{D1A2B3C4-E5F6-7890-1234-567890ABCDEF}} 
AppName=Wrought Iron
AppVersion=1.0.0
AppPublisher=Divyanshu Sharma
DefaultDirName={autopf}\Wrought Iron
DefaultGroupName=Wrought Iron
AllowNoIcons=yes
LicenseFile=license.rtf
OutputBaseFilename=WroughtIronInstaller
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin 
ArchitecturesInstallIn64BitMode=x64 
ChangesEnvironment=yes 

[Files]
Source: "dist\wrought_iron\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Tasks]
Name: "addtopath"; Description: "Add Wrought Iron to system PATH"; GroupDescription: "Additional tasks:"; Flags: unchecked

[Icons]
Name: "{group}\Wrought Iron"; Filename: "{app}\wi.exe"

[Code]
const
  PathRegistryKey = 'SOFTWARE\Wrought Iron';
  PathRegistryValueName = 'AddedToPath';

function PathExists(const Path: String): Boolean;
var
  OldPath: String;
begin
  Result := False;
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', OldPath) then
  begin
    OldPath := '';
  end;
  Result := Pos(Uppercase(AddBackslash(Path)), Uppercase(AddBackslash(OldPath))) > 0;
end;

function AddToPath(const Path: String): Boolean;
var
  OldPath: String;
  NewPath: String;
begin
  Result := False;
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', OldPath) then
    OldPath := '';

  if not PathExists(Path) then 
  begin
    NewPath := OldPath;
    if (NewPath <> '') and (NewPath[Length(NewPath)] <> ';') then
      NewPath := NewPath + ';';
    
    NewPath := NewPath + Path;

    if RegWriteStringValue(HKEY_LOCAL_MACHINE, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', NewPath) then
    begin
      Result := True;
    end;
  end else begin
    Log(Format('Path "%s" already exists in system PATH.', [Path]));
    Result := True; 
  end;
end;

function RemoveFromPath(const Path: String): Boolean;
var
  OldPath: String;
  NewPath: String;
  PathToRemove: String;
  PathItems: TStringList;
  i: Integer;
begin
  Result := False;
  PathToRemove := Uppercase(RemoveBackslash(Path));

  if not RegQueryStringValue(HKEY_LOCAL_MACHINE, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', OldPath) then
    OldPath := '';

  PathItems := TStringList.Create;
  try
    StringChange(OldPath, ';', #13#10); 
    PathItems.Text := OldPath;

    NewPath := '';
    for i := 0 to PathItems.Count - 1 do
    begin
      if Uppercase(RemoveBackslash(PathItems[i])) <> PathToRemove then
      begin
        if NewPath <> '' then
          NewPath := NewPath + ';';
        NewPath := NewPath + PathItems[i];
      end;
    end;

    if CompareText(OldPath, NewPath) <> 0 then
    begin
      if RegWriteStringValue(HKEY_LOCAL_MACHINE, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', NewPath) then
      begin
        Result := True;
      end;
    end else begin
      Log(Format('Path "%s" was not found in system PATH or no change needed.', [Path]));
      Result := True;
    end;
  finally
    PathItems.Free;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    if IsTaskSelected('addtopath') then
    begin
      if AddToPath(ExpandConstant('{app}')) then
      begin
        Log('Successfully added {app} to PATH');
        RegWriteStringValue(HKEY_LOCAL_MACHINE, PathRegistryKey, PathRegistryValueName, '1');
      end
      else
        Log('Failed to add {app} to PATH.');
    end;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  AddedToPathFlag: String;
begin
  if CurUninstallStep = usPostUninstall then
  begin
    if RegQueryStringValue(HKEY_LOCAL_MACHINE, PathRegistryKey, PathRegistryValueName, AddedToPathFlag) then
    begin
      if AddedToPathFlag = '1' then
      begin
        Log('Detected that {app} was added to PATH during installation. Attempting to remove...');
        if RemoveFromPath(ExpandConstant('{app}')) then
        begin
          Log('Successfully removed {app} from PATH');
          RegDeleteValue(HKEY_LOCAL_MACHINE, PathRegistryKey, PathRegistryValueName);
        end
        else
          Log('Failed to remove {app} from PATH.');
      end;
    end;
  end;
end;