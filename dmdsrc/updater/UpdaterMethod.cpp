/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include <iostream>
#include <boost/bind.hpp>

#include "UpdaterMethod.h"
#include "UpdaterParameters.h"
#include "system/Channel.h"
#include "system/ChannelOut.h"
#include "system/StdOut.h"

using namespace std;
using namespace boost;

UpdaterMethod::UpdaterMethod(const boost::function0<int>& method, const std::string& description)
	:	method(method), description(description)
{
}

UpdaterMethod::UpdaterMethod(const boost::function0<void>& method, const std::string& description)
	:	method(bind(&UpdaterMethod::VoidFunctionAdaptor, method)), description(description)
{
}

int UpdaterMethod::Run()
{
	UpdaterParameters::LogOutput() << lock << description << "... ";
	int method_returned = -1;
	try {
		method_returned = method();
	} catch(std::exception& e) {
		UpdaterParameters::LogOutput() << "[ FAILED ]" << unlock;
		UpdaterParameters::LogOutput() << lock << "ERROR: " << e.what() << unlock;
		return -1;
	}

	if(method_returned < 0)
		UpdaterParameters::LogOutput() << "[ FAILED ]";
	else
		UpdaterParameters::LogOutput() << "[ OK ]";
	UpdaterParameters::LogOutput() << unlock;
	return method_returned;
}

int UpdaterMethod::VoidFunctionAdaptor(boost::function0<void> method)
{
	method();
	return 1;
}
