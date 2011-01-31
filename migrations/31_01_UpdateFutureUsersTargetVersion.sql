update
  future.Users,
  usersettings.UserUpdateInfo
set
  Users.SaveAFDataFiles = UserUpdateInfo.SaveAFDataFiles,
  Users.TargetVersion = UserUpdateInfo.TargetVersion
where
  Users.Id = UserUpdateInfo.UserId;