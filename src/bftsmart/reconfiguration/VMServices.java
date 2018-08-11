/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration;

import bftsmart.tom.util.KeyLoader;
import java.security.Provider;

/**
 *
 * @author Andre Nogueira
 */

public abstract class VMServices {
    
    public abstract KeyLoader getKeyLoader();
    public abstract Provider getProvider();
    
    public void addServer(int id, String ipAddress, int port) {
        
        ViewManager viewManager = new ViewManager(getKeyLoader(), getProvider());
        
        viewManager.addServer(id, ipAddress,port);
        
        execute(viewManager);

    }
    
    public void removeServer (int id) {
        
        ViewManager viewManager = new ViewManager(getKeyLoader(), getProvider());
        
        viewManager.removeServer(id);
        
        execute(viewManager);

    }
    
    private void execute(ViewManager viewManager) {
        
        viewManager.executeUpdates();
        
        viewManager.close();
    }
}