import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import {HomeComponent} from "./screens/home/home.component";
import {RouterModule} from "@angular/router";
import {HomeModule} from "./screens/home/home.module";
import {SettingModule} from "./screens/setting/setting.module";
import {SettingComponent} from "./screens/setting/setting.component";
import {NotfoundComponent} from "./shared/notfound/notfound.component";

@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    RouterModule.forRoot([
      {path: '', component: HomeComponent},
      {path: 'settings', component: SettingComponent},
      {path: '**', component: NotfoundComponent}
    ]),
    HomeModule,
    SettingModule,
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
