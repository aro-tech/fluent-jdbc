# fluent-jdbc
An idea for fluent, lambda-friendly handling of SQL requests in Java.
No ORM!

Requires Java 8 (or higher)


##Latest release

Currently just in the idea phase.
There's no notion yet of transactions, and the tests against a real database are not yet implemented.  The code was pulled from another project which had such tests for the business logic.

##Example usage 
```
	public UserAccount retrieveByLogin(String login) {
		String sql = "SELECT login, password, locale FROM user_account WHERE login=?";
		ResultSetReader<UserAccount> resultReader = resultSet -> {
			if (resultSet.next()) {
				String loginVal = resultSet.getString(1);
				String passwordVal = resultSet.getString(2);
				String localeTag = resultSet.getString(3);
				Locale localeVal = Locale.getDefault();
				if (null != localeTag && localeTag.length() > 0) {
					localeVal = Locale.forLanguageTag(localeTag);
				}
				return new UserAccount(loginVal, passwordVal, localeVal);
			}
			return null;
		};
		return RequestBuilder.returning(UserAccount.class).withLogger(logger)
				.withConnectionProvider(connectionProvider).withSQL(sql)
				.addParam(login).withResultReader(resultReader).execute();
	}
```	

		
 
##Blog
[![The Green Bar](https://img.shields.io/badge/My_Blog:-The_Green_Bar-brightgreen.svg)](https://thegreenbar.wordpress.com/)
